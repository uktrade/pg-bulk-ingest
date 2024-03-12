import uuid
import logging
from collections import deque, defaultdict
from contextlib import contextmanager
from enum import Enum

import sqlalchemy as sa
import json
from io import IOBase

from pg_force_execute import pg_force_execute
from to_file_like_obj import to_file_like_obj

try:
    from psycopg2 import sql as sql2
except ImportError:
    sql2 = None

try:
    from psycopg import sql as sql3
except ImportError:
    sql3 = None


class Upsert:
    OFF = '__OFF__'
    IF_PRIMARY_KEY = '__IF_PRIMARY_KEY__'


class Delete:
    OFF = '__OFF__'
    BEFORE_FIRST_BATCH = '__BEFORE_FIRST_BATCH__'


class HighWatermark:
    LATEST = '__LATEST__'
    EARLIEST = '__EARLIEST__'


class Visibility:
    AFTER_EACH_BATCH = '__AFTER_EACH_BATCH__'


def ingest(
        conn, metadata, batches, high_watermark=HighWatermark.LATEST,
        visibility=Visibility.AFTER_EACH_BATCH, upsert=Upsert.IF_PRIMARY_KEY, delete=Delete.OFF,
        get_pg_force_execute=lambda conn, logger: pg_force_execute(conn, logger=logger),
        on_before_visible=lambda conn, ingest_table, batch_metadata: None, logger=logging.getLogger("pg_bulk_ingest"),
        max_rows_per_table_buffer=10000,
):

    def temp_relation_name():
        return f"_tmp_{uuid.uuid4().hex}"
    
    def sql_and_copy_from_stdin(driver):
        # Supporting both psycopg2 and Psycopg 3. Psycopg 3 has a nicer
        # COPY ... FROM STDIN API via write_row that handles escaping,
        # but for consistency with Psycopg 2 we don't use it
        def copy_from_stdin2(cursor, query, f):
            cursor.copy_expert(query, f, size=65536)

        def copy_from_stdin3(cursor, query, f):
            with cursor.copy(query) as copy:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    copy.write(chunk)

        return {
            'psycopg2': (sql2, copy_from_stdin2),
            'psycopg': (sql3, copy_from_stdin3),
        }[driver]

    def bind_identifiers(sql, conn, query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    def save_comment(sql, conn, schema, table, comment):
        conn.execute(sa.text(sql.SQL('''
             COMMENT ON TABLE {schema}.{table} IS {comment}
        ''').format(
            schema=sql.Identifier(target_table.schema),
            table=sql.Identifier(target_table.name),
            comment=sql.Literal(comment),
        ).as_string(conn.connection.driver_connection)))

    def create_first_batch_ingest_table_if_necessary(sql, conn, live_table, target_table):
        must_create_ingest_table = False

        if delete == Delete.BEFORE_FIRST_BATCH:
            must_create_ingest_table = True

        if not must_create_ingest_table:
            logger.info('Finding existing columns of %s.%s', str(target_table.schema), str(target_table.name))
            # Reflection can return a different server_default even if client code didn't set it
            for column in live_table.columns.values():
                column.server_default = None
            logger.info('Existing columns of %s.%s are %s', str(target_table.schema), str(target_table.name), list(live_table.columns))

            live_table_column_names = set(live_table.columns.keys())

            # postgresql_include should be ignored if empty, but it seems to "sometimes" appear
            # both in reflected tables, and when
            def get_dialect_kwargs(index):
                dialect_kwargs = dict(index.dialect_kwargs)
                if 'postgresql_include' in dialect_kwargs and not dialect_kwargs['postgresql_include']:
                    del dialect_kwargs['postgresql_include']
                return dialect_kwargs

            # Don't migrate if indexes only differ by name
            # Only way discovered is to temporarily change the name in each index name object
            @contextmanager
            def ignored_name(indexes):
                names = [index.name for index in indexes]
                for index in indexes:
                    index.name = '__IGNORE__'
                try:
                    yield
                finally:
                    for name, index in zip(names, indexes):
                        index.name = name
            with \
                    ignored_name(live_table.indexes), \
                    ignored_name(target_table.indexes):
                indexes_live_repr = set(repr(index) + '--' + str(get_dialect_kwargs(index)) for index in live_table.indexes)
                indexes_target_repr = set(repr(index) + '--' + str(get_dialect_kwargs(index)) for index in target_table.indexes)

            columns_target = tuple(
                (col.name, repr(col.type), col.nullable, col.primary_key, col.unique) for col in target_table.columns.values()
            )
            columns_live = tuple(
                (col.name, repr(col.type), col.nullable, col.primary_key, col.unique) for col in live_table.columns.values()
            )

            indexes_target_repr != indexes_live_repr or columns_target != columns_live
            must_create_ingest_table = indexes_target_repr != indexes_live_repr or columns_target != columns_live

        if not must_create_ingest_table:
            return target_table

        logger.info('Ingesting via new ingest table')
        ingest_metadata = sa.MetaData()
        ingest_table = sa.Table(
            temp_relation_name(),
            ingest_metadata,
            *(
                sa.Column(column.name, column.type, unique=column.unique, nullable=column.nullable, primary_key=column.primary_key)
                for column in target_table.columns
            ),
            schema=target_table.schema
        )
        ingest_metadata.create_all(conn)

        return ingest_table

    def split_batch_into_tables(live_tables, combined_batch):
        ingested_tables = set()
        queues = defaultdict(deque)
        current_queue = None
        current_table = None

        batch_iter = iter(batch)

        def batch_for_current_table_until_a_queue_full():
            nonlocal current_queue, current_table

            while current_queue:
                table, row = current_queue.popleft()
                ingested_tables.add(table)
                yield table, row 

            while True:
                try:
                    table, row = next(batch_iter)
                except StopIteration:
                    current_queue = None
                    current_table = None
                    break

                if table is current_table:
                    ingested_tables.add(table)
                    yield table, row
                else:
                    queue = queues[table]
                    queue.append((table, row))
                    if len(queue) >= max_rows_per_table_buffer:
                        current_queue = queue
                        current_table = table
                        break

        # Bootstrap, choosing the first table from the first row
        try:
            table, row = next(batch_iter)
        except StopIteration:
            pass
        else:
            current_queue = deque()
            current_queue.append((table, row))
            current_table = table

            # Yield table batches
            while current_queue:
                yield current_table, live_tables[current_table], batch_for_current_table_until_a_queue_full()

            # Yield data from remaining queues
            for table, queue in queues.items():
                current_queue = queue
                current_table = table
                yield current_table, live_tables[current_table], batch_for_current_table_until_a_queue_full()

        # Yield empty table batch for tables we haven't ingested anything into at all
        for table in (set(live_tables.keys()) - ingested_tables):
            yield table, live_tables[table], ()

    def csv_copy(sql, copy_from_stdin, conn, user_facing_table, batch_table, rows):

        def get_converter(sa_type):

            def escape_array_item(text):
                return text.replace('\\', '\\\\').replace('"', '\\"')

            def escape_string(text):
                return (
                    text.replace('\\', '\\\\')
                    .replace('\n', '\\n')
                    .replace('\r', '\\r')
                    .replace('\t', '\\t')
                )

            null = '\\N'

            if isinstance(sa_type, sa.Integer):
                return lambda v: (null if v is None else str(int(v)))
            elif isinstance(sa_type, sa.JSON):
                return lambda v: (null if v is None else escape_string(json.dumps(v)))
            elif isinstance(sa_type, sa.ARRAY):
                return lambda v: (
                    null
                    if v is None
                    else escape_string(
                        '{'
                        + (
                            ','.join(
                                (
                                    'NULL'
                                    if item is None
                                    else ('"' + escape_array_item(str(item)) + '"')
                                )
                                for item in v
                            )
                        )
                        + '}',
                    )
                )
            elif isinstance(sa_type, sa.dialects.postgresql.BYTEA):
                return lambda v: (null if v is None else r'\x' + v.hex().upper())
            else:
                return lambda v: (null if v is None else escape_string(str(v)))

        converters = tuple(get_converter(column.type) for column in batch_table.columns)
        db_rows = (
            '\t'.join(converter(value) for (converter,value) in zip(converters, row)) + '\n'
            for row_table, row in rows
            if row_table is user_facing_table
        )
        with conn.connection.driver_connection.cursor() as cursor:
            copy_from_stdin(cursor, str(bind_identifiers(sql, conn, "COPY {}.{} FROM STDIN", batch_table.schema, batch_table.name)), to_file_like_obj(db_rows, str))

    sql, copy_from_stdin = sql_and_copy_from_stdin(conn.engine.driver)

    conn.begin()

    # Create the target tables without indexes, which are added later with
    # with logic to avoid duplicate names
    for target_table in metadata.tables.values():
        logger.info("Creating target table %s if it doesn't already exist", target_table)
        if target_table.schema not in sa.inspect(conn).get_schema_names():
            conn.execute(sa.schema.CreateSchema(target_table.schema))
        initial_table_metadata = sa.MetaData()
        initial_table = sa.Table(
            target_table.name,
            initial_table_metadata,
            *(
                sa.Column(column.name, column.type, nullable=column.nullable, unique=column.unique)
                for column in target_table.columns
            ),
            schema=target_table.schema
        )
        initial_table_metadata.create_all(conn)
        logger.info('Target table %s created or already existed', target_table)

    target_tables = tuple(metadata.tables.values())
    live_tables = {
        target_table: sa.Table(target_table.name, sa.MetaData(), schema=target_table.schema, autoload_with=conn)
        for target_table in target_tables
    }

    logger.info('Finding high-watermark of %s', str(target_tables[0].name))
    comment = conn.execute(sa.text(sql.SQL('''
         SELECT obj_description((quote_ident({schema}) || '.' || quote_ident({table}))::regclass, 'pg_class')
    ''').format(
        schema=sql.Literal(target_tables[0].schema),
        table=sql.Literal(target_tables[0].name),
    ).as_string(conn.connection.driver_connection))).fetchall()[0][0]
    try:
        comment_parsed = json.loads(comment)
    except (TypeError, ValueError):
        comment_parsed = {}

    if high_watermark == HighWatermark.LATEST:
        high_watermark_value = comment_parsed.get('pg-bulk-ingest', {}).get('high-watermark')
    elif high_watermark == HighWatermark.EARLIEST:
        high_watermark_value = None
    else:
        high_watermark_value = high_watermark
    logger.info('High-watermark of %s.%s is %s', str(target_tables[0].schema), str(target_tables[0].name), high_watermark_value)

    ingested_target_tables = set()

    for high_watermark_value, batch_metadata, batch in batches(high_watermark_value):

        batch_ingest_tables = {}

        for target_table, live_table, table_batch in split_batch_into_tables(live_tables, batch):
            if target_table in batch_ingest_tables:
                # Always ingest into the same table for a batch
                ingest_table = batch_ingest_tables[target_table]
            elif target_table not in ingested_target_tables:
                # The first time we ingest into a table, we might need to create a new table, and maybe copy
                # existing data into this new table. But only the first time, so we maintain a bit of state
                # to avoid doing it again
                ingest_table = create_first_batch_ingest_table_if_necessary(sql, conn, live_table, target_table)

                if ingest_table is not target_table and delete == Delete.OFF:
                    target_table_column_names = tuple(col.name for col in target_table.columns)
                    columns_to_select = tuple(col for col in live_table.columns.values() if col.name in target_table_column_names)
                    conn.execute(sa.insert(ingest_table).from_select(
                        tuple(col.name for col in columns_to_select),
                        sa.select(*columns_to_select),
                    ))
                ingested_target_tables.add(target_table)
                batch_ingest_tables[target_table] = ingest_table
            else:
                # Otherwise ingest directly into the target table
                ingest_table = target_table
                batch_ingest_tables[target_table] = ingest_table

            logger.info('Ingesting batch %s with high watermark value %s', batch_metadata, high_watermark_value)

            is_upsert = upsert == Upsert.IF_PRIMARY_KEY and any(column.primary_key for column in target_table.columns.values())
            if not is_upsert:
                logger.info('Ingesting without upsert')
                csv_copy(sql, copy_from_stdin, conn, target_table, ingest_table, table_batch)
            else:
                logger.info('Ingesting with upsert')
                # Create a batch table, and ingest into it
                logger.info('Creating and ingesting into batch table')
                batch_db_metadata = sa.MetaData()
                batch_table = sa.Table(
                    temp_relation_name(),
                    batch_db_metadata,
                    *(
                        sa.Column(column.name, column.type)
                        for column in ingest_table.columns
                    ),
                    schema=ingest_table.schema
                )
                batch_db_metadata.create_all(conn)
                csv_copy(sql, copy_from_stdin, conn, target_table, batch_table, table_batch)
                logger.info('Ingestion into batch table complete')

                # check and remove any duplicates in the batch
                logger.info('Check and remove any duplicates in the batch table')
                dedup_query = sql.SQL('''
                    DELETE FROM {schema}.{batch_table} a USING (
                        SELECT MAX(ctid) as ctid, {pk_columns}
                        FROM {schema}.{batch_table}
                        GROUP BY {pk_columns} HAVING COUNT(*) > 1
                    ) b
                    WHERE {clause}
                    AND a.ctid <> b.ctid
                ''').format(
                    schema=sql.Identifier(batch_table.schema),
                    batch_table=sql.Identifier(batch_table.name),
                    pk_columns=sql.SQL(',').join((sql.Identifier(column.name) for column in ingest_table.columns if column.primary_key)),
                    clause=sql.SQL(' and ').join((sql.SQL('a.{} = b.{}').format(sql.Identifier(column.name), sql.Identifier(column.name)) for column in ingest_table.columns if column.primary_key))
                )
                conn.execute(sa.text(dedup_query.as_string(conn.connection.driver_connection)))
                logger.info('Deduplicating batch table complete')

                # Copy from that batch table into the target table, using
                # ON CONFLICT to update any existing rows
                logger.info('Copying from batch table to target table')
                insert_query = sql.SQL('''
                    INSERT INTO {schema}.{table}
                    SELECT * FROM {schema}.{batch_table}
                    ON CONFLICT({primary_keys})
                    DO UPDATE SET {updates}
                ''').format(
                    schema=sql.Identifier(ingest_table.schema),
                    table=sql.Identifier(ingest_table.name),
                    batch_table=sql.Identifier(batch_table.name),
                    primary_keys=sql.SQL(',').join((sql.Identifier(column.name) for column in ingest_table.columns if column.primary_key)),
                    updates=sql.SQL(',').join(sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(column.name), sql.Identifier(column.name)) for column in ingest_table.columns),
                )
                conn.execute(sa.text(insert_query.as_string(conn.connection.driver_connection)))
                logger.info('Copying from batch table to target table complete')

                # Drop the batch table
                logger.info('Dropping batch table')
                batch_db_metadata.drop_all(conn)
                logger.info('Batch table dropped')

        for target_table, ingest_table in batch_ingest_tables.items():
            logger.info("Creating indexes for %s.%s", str(target_table.schema), str(target_table.name))
            if ingest_table is not target_table:
                for index in target_table.indexes:
                    logger.info("Creating index %s", index)
                    sa.Index(
                        temp_relation_name(),
                        *(ingest_table.columns[column.name] for column in index.columns),
                        **index.dialect_kwargs,
                    ).create(bind=conn)

            logger.info("Copying privileges for %s.%s", str(target_table.schema), str(target_table.name))
            grantees = conn.execute(sa.text(sql.SQL('''
                SELECT grantee
                FROM information_schema.role_table_grants
                WHERE table_schema = {schema} AND table_name = {table}
                AND privilege_type = 'SELECT'
                AND grantor != grantee
            ''').format(schema=sql.Literal(target_table.schema), table=sql.Literal(target_table.name))
                .as_string(conn.connection.driver_connection)
            )).fetchall()
            for grantee in grantees:
                conn.execute(sa.text(sql.SQL('GRANT SELECT ON {schema_table} TO {user}')
                    .format(
                        schema_table=sql.Identifier(ingest_table.schema, ingest_table.name),
                        user=sql.Identifier(grantee[0]),
                    )
                    .as_string(conn.connection.driver_connection))
                )

        for ingest_table in batch_ingest_tables.values():
            logger.info('Calling on_before_visible callback')
            on_before_visible(conn, ingest_table, batch_metadata)
            logger.info('Calling of on_before_visible callback complete')

        logger.info("Renaming tables if necessary")
        for target_table, ingest_table in batch_ingest_tables.items():
            if ingest_table is not target_table:
                with get_pg_force_execute(conn, logger):
                    target_table.drop(conn)
                    rename_query = sql.SQL('''
                        ALTER TABLE {schema}.{ingest_table}
                        RENAME TO {target_table}
                    ''').format(
                        schema=sql.Identifier(ingest_table.schema),
                        ingest_table=sql.Identifier(ingest_table.name),
                        target_table=sql.Identifier(target_table.name),
                    )
                    conn.execute(sa.text(rename_query.as_string(conn.connection.driver_connection)))

        if callable(high_watermark_value):
            high_watermark_value = high_watermark_value()

        comment_parsed['pg-bulk-ingest'] = comment_parsed.get('pg-bulk-ingest', {})
        comment_parsed['pg-bulk-ingest']['high-watermark'] = high_watermark_value
        save_comment(sql, conn, target_tables[0].schema, target_tables[0].name, json.dumps(comment_parsed))

        conn.commit()
        logger.info('Ingestion of batch %s with high watermark value %s complete', batch_metadata, high_watermark_value)
        conn.begin()

    conn.commit()

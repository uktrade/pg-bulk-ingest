import uuid
import logging
from contextlib import contextmanager
from enum import Enum

import sqlalchemy as sa
import json
from io import IOBase

from pg_force_execute import pg_force_execute

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
        on_before_visible=lambda conn, batch_metadata: None, logger=logging.getLogger("pg_bulk_ingest"),
):

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

    def migrate_if_necessary(sql, conn, target_table, comment):
        logger.info('Finding existing columns of %s.%s', target_table.schema, target_table.name)
        live_table = sa.Table(target_table.name, sa.MetaData(), schema=target_table.schema, autoload_with=conn)
        # Reflection can return a different server_default even if client code didn't set it
        for column in live_table.columns.values():
            column.server_default = None
        logger.info('Existing columns of %s.%s are %s', target_table.schema, target_table.name, list(live_table.columns))

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
            (col.name, repr(col.type), col.nullable, col.primary_key) for col in target_table.columns.values()
        )
        columns_live = tuple(
            (col.name, repr(col.type), col.nullable, col.primary_key) for col in live_table.columns.values()
        )
        columns_to_drop = tuple(col for col in columns_live if col not in columns_target)
        columns_to_add = tuple(col for col in columns_target if col not in columns_live)
        columns_to_drop_obj = tuple(live_table.columns[col[0]] for col in columns_to_drop)
        columns_to_add_obj = tuple(target_table.columns[col[0]] for col in columns_to_add)

        must_migrate = indexes_target_repr != indexes_live_repr or columns_target != columns_live

        migrate_in_place = must_migrate \
            and not indexes_target_repr != indexes_live_repr \
            and all(col.nullable for col in columns_to_add_obj) \
            and all(not col.primary_key for col in columns_to_drop_obj) \
            and all(not col.primary_key for col in columns_to_add_obj) \
            and all(
                i == len(columns_target) - 1 or col not in columns_to_add or columns_target[i+1] in columns_to_add
                for i, col in enumerate(columns_target)
            ) # All columns to add are at the end

        via_migration_table = must_migrate and not migrate_in_place

        if migrate_in_place:
            logger.info('Migrating in place')
            alter_query = sql.SQL('''
                ALTER TABLE {schema}.{table}
                {statements}
            ''').format(
                schema=sql.Identifier(target_table.schema),
                table=sql.Identifier(target_table.name),
                statements=sql.SQL(',').join(
                    tuple(
                        sql.SQL('DROP COLUMN {column_name}').format(
                            column_name=sql.Identifier(col.name),
                        )
                        for col in columns_to_drop_obj
                    )
                    + tuple(
                        sql.SQL('ADD COLUMN {column_name} {column_type}').format(
                            column_name=sql.Identifier(col.name),
                            column_type=sql.SQL(col.type.compile(conn.engine.dialect)),
                        )
                        for col in columns_to_add_obj
                    )
                )
            )
            with get_pg_force_execute(conn, logger):
                conn.execute(sa.text(alter_query.as_string(conn.connection.driver_connection)))

            conn.commit()
            conn.begin()

        elif via_migration_table:
            logger.info('Migrating via migration table')
            migration_metadata = sa.MetaData()
            migration_table = sa.Table(
                uuid.uuid4().hex,
                migration_metadata,
                *(
                    sa.Column(column.name, column.type, nullable=column.nullable, primary_key=column.primary_key)
                    for column in target_table.columns
                ),
                schema=target_table.schema
            )
            migration_metadata.create_all(conn)
            target_table_column_names = tuple(col.name for col in target_table.columns)
            columns_to_select = tuple(col for col in live_table.columns.values() if col.name in target_table_column_names)

            conn.execute(sa.insert(migration_table).from_select(
                tuple(col.name for col in columns_to_select),
                sa.select(*columns_to_select),
            ))

            for index in target_table.indexes:
                sa.Index(
                    uuid.uuid4().hex,
                    *(migration_table.columns[column.name] for column in index.columns),
                    **index.dialect_kwargs,
                ).create(bind=conn)

            grantees = conn.execute(sa.text(sql.SQL('''
                SELECT grantee
                FROM information_schema.role_table_grants
                WHERE table_schema = {schema} AND table_name = {table}
                AND privilege_type = 'SELECT'
                AND grantor != grantee
            ''').format(schema=sql.Literal(target_table.schema), table=sql.Literal(target_table.name))
                .as_string(conn.connection.driver_connection)
            )).fetchall()
            with get_pg_force_execute(conn, logger):
                target_table.drop(conn)
                rename_query = sql.SQL('''
                    ALTER TABLE {schema}.{migration_table}
                    RENAME TO {target_table}
                ''').format(
                    schema=sql.Identifier(migration_table.schema),
                    migration_table=sql.Identifier(migration_table.name),
                    target_table=sql.Identifier(target_table.name),
                )
                conn.execute(sa.text(rename_query.as_string(conn.connection.driver_connection)))

            for grantee in grantees:
                conn.execute(sa.text(sql.SQL('GRANT SELECT ON {schema_table} TO {user}')
                    .format(
                        schema_table=sql.Identifier(target_table.schema, target_table.name),
                        user=sql.Identifier(grantee[0]),
                    )
                    .as_string(conn.connection.driver_connection))
                )

            save_comment(sql, conn, target_table.schema, target_table.name, comment)

            conn.commit()
            conn.begin()

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
            else:
                return lambda v: (null if v is None else escape_string(str(v)))

        def to_file_like_obj(iterable, base):
            chunk = base()
            offset = 0
            it = iter(iterable)

            def up_to_iter(size):
                nonlocal chunk, offset

                while size:
                    if offset == len(chunk):
                        try:
                            chunk = next(it)
                        except StopIteration:
                            break
                        else:
                            offset = 0
                    to_yield = min(size, len(chunk) - offset)
                    offset = offset + to_yield
                    size -= to_yield
                    yield chunk[offset - to_yield : offset]

            class FileLikeObj(IOBase):
                def readable(self):
                    return True

                def read(self, size=-1):
                    return base().join(
                        up_to_iter(float('inf') if size is None or size < 0 else size)
                    )

            return FileLikeObj()

        converters = tuple(get_converter(column.type) for column in batch_table.columns)
        db_rows = (
            '\t'.join(converter(value) for (converter,value) in zip(converters, row)) + '\n'
            for row_table, row in rows
            if row_table is user_facing_table
        )
        with conn.connection.driver_connection.cursor() as cursor:
            copy_from_stdin(cursor, str(bind_identifiers(sql, conn, "COPY {}.{} FROM STDIN", batch_table.schema, batch_table.name)), to_file_like_obj(db_rows, str))

    if len(metadata.tables) > 1:
        raise ValueError('Only one table supported')

    sql, copy_from_stdin = sql_and_copy_from_stdin(conn.engine.driver)

    conn.begin()

    # Create the target table without indexes, which are added later with
    # with logic to avoid duplicate names
    target_table = next(iter(metadata.tables.values()))
    logger.info("Creating target table %s if it does't already exist", target_table)
    conn.execute(bind_identifiers(sql, conn, '''
        CREATE SCHEMA IF NOT EXISTS {}
    ''', target_table.schema))
    initial_table_metadata = sa.MetaData()
    initial_table = sa.Table(
        target_table.name,
        initial_table_metadata,
        *(
            sa.Column(column.name, column.type)
            for column in target_table.columns
        ),
        schema=target_table.schema
    )
    initial_table_metadata.create_all(conn)
    logger.info('Target table %s created or already existed', target_table)

    is_upsert = upsert == Upsert.IF_PRIMARY_KEY and any(column.primary_key for column in target_table.columns.values())

    logger.info('Finding high-watermark of %s.%s', target_table.schema, target_table.name)
    comment = conn.execute(sa.text(sql.SQL('''
         SELECT obj_description((quote_ident({schema}) || '.' || quote_ident({table}))::regclass, 'pg_class')
    ''').format(
        schema=sql.Literal(target_table.schema),
        table=sql.Literal(target_table.name),
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
    logger.info('High-watermark of %s.%s is %s', target_table.schema, target_table.name, high_watermark_value)

    migrate_if_necessary(sql, conn, target_table, comment)

    for i, (high_watermark_value, batch_metadata, batch) in enumerate(batches(high_watermark_value)):
        logger.info('Ingesting batch %s with high watermark value %s', batch_metadata, high_watermark_value)

        if i == 0 and delete == Delete.BEFORE_FIRST_BATCH:
            logger.info('Deleting target table %s', target_table)
            conn.execute(sa.delete(target_table))
            logger.info('Target table %s deleted', target_table)

        if not is_upsert:
            logger.info('Ingesting without upsert')
            csv_copy(sql, copy_from_stdin, conn, target_table, target_table, batch)
        else:
            logger.info('Ingesting with upsert')
            # Create a batch table, and ingest into it
            logger.info('Creating and ingesting into batch table')
            batch_db_metadata = sa.MetaData()
            batch_table = sa.Table(
                uuid.uuid4().hex,
                batch_db_metadata,
                *(
                    sa.Column(column.name, column.type)
                    for column in target_table.columns
                ),
                schema=target_table.schema
            )
            batch_db_metadata.create_all(conn)
            csv_copy(sql, copy_from_stdin, conn, target_table, batch_table, batch)
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
                schema=sql.Identifier(target_table.schema),
                batch_table=sql.Identifier(batch_table.name),
                pk_columns=sql.SQL(',').join((sql.Identifier(column.name) for column in target_table.columns if column.primary_key)),
                clause=sql.SQL(' and ').join((sql.SQL('a.{} = b.{}').format(sql.Identifier(column.name), sql.Identifier(column.name)) for column in target_table.columns if column.primary_key))
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
                schema=sql.Identifier(target_table.schema),
                table=sql.Identifier(target_table.name),
                batch_table=sql.Identifier(batch_table.name),
                primary_keys=sql.SQL(',').join((sql.Identifier(column.name) for column in target_table.columns if column.primary_key)),
                updates=sql.SQL(',').join(sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(column.name), sql.Identifier(column.name)) for column in target_table.columns),
            )
            conn.execute(sa.text(insert_query.as_string(conn.connection.driver_connection)))
            logger.info('Copying from batch table to target table complete')

            # Drop the batch table
            logger.info('Dropping batch table')
            batch_db_metadata.drop_all(conn)
            logger.info('Batch table dropped')

        comment_parsed['pg-bulk-ingest'] = comment_parsed.get('pg-bulk-ingest', {})
        comment_parsed['pg-bulk-ingest']['high-watermark'] = high_watermark_value
        save_comment(sql, conn, target_table.schema, target_table.name, json.dumps(comment_parsed))

        logger.info('Calling on_before_visible callback')
        on_before_visible(conn, batch_metadata)
        logger.info('Calling of on_before_visible callback complete')
        conn.commit()
        logger.info('Ingestion of batch %s with high watermark value %s complete', batch_metadata, high_watermark_value)
        conn.begin()

    conn.commit()

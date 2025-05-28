import uuid
import logging
import math
import typing
import types
from collections import deque, defaultdict
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import BYTEA
import json

from pg_force_execute import pg_force_execute
from to_file_like_obj import to_file_like_obj

sql2: types.ModuleType
sql3: types.ModuleType

try:
    from psycopg2 import sql as sql2_module
    sql2 = sql2_module
except ImportError:
    pass

try:
    from psycopg import sql as sql3_module
    sql3 = sql3_module
except ImportError:
    pass


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
    conn:typing.Any, metadata:typing.Any, batches:typing.Any, high_watermark:str=HighWatermark.LATEST,
    visibility:str=Visibility.AFTER_EACH_BATCH, upsert:str=Upsert.IF_PRIMARY_KEY, delete:str=Delete.OFF,
    get_pg_force_execute:typing.Any=lambda conn, logger: pg_force_execute(conn, logger=logger),
    on_before_visible:typing.Any=lambda conn, ingest_table, batch_metadata: None, logger: logging.Logger=logging.getLogger("pg_bulk_ingest"),
    max_rows_per_table_buffer:int=10000,
) -> None:

    def temp_relation_name() -> str:
        return f"_tmp_{uuid.uuid4().hex}"

    def bind_identifiers(sql: typing.Any, conn: typing.Any, query_str: str, *identifiers: typing.Optional[str]) -> sa.sql.elements.TextClause:
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    def save_comment(sql: typing.Any, conn: typing.Any, schema: typing.Any, table: sa.Table, comment: str) -> None:
        conn.execute(sa.text(sql.SQL('''
             COMMENT ON TABLE {schema}.{table} IS {comment}
        ''').format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            comment=sql.Literal(comment),
        ).as_string(conn.connection.driver_connection)))

    def get_dependent_views(sql: typing.Any, conn: typing.Any, schema: str, table: str) -> typing.List[typing.Dict[str, str]]:
        query = sql.SQL(_VIEWS_SQL).format(
            schema=sql.Literal(schema),
            table=sql.Literal(table)
        )

        results = conn.execute(sa.text(query.as_string(conn.connection.driver_connection))).fetchall()
        return [
            {
                'fully_qualified_name': fully_qualified_name,
                'column_names': column_names,
                'null_typed_columns': null_typed_columns,
                'definition': f'CREATE OR REPLACE VIEW {fully_qualified_name} AS {definition}',
                'is_materialized': is_materialized,
                'materialized_definition': f'CREATE MATERIALIZED VIEW {fully_qualified_name} AS {definition}',
                'quoted_grantees': quoted_grantees
            }
            for fully_qualified_name, column_names, null_typed_columns, definition, is_materialized, quoted_grantees in results
        ]

    def drop_views(sql: typing.Any, conn: typing.Any, views: typing.List[typing.Dict[str, str]]) -> None:
        for view in views:
            view_type = "MATERIALIZED VIEW" if view["is_materialized"] else "VIEW"
            logger.info("Dropping %s %s", view_type, view["fully_qualified_name"])
            query = sql.SQL('''
                DROP {view_type} IF EXISTS {fully_qualified_name} CASCADE
            ''').format(
                view_type=sql.SQL(view_type),
                fully_qualified_name=sql.SQL(view["fully_qualified_name"])
            )
            conn.execute(sa.text(query.as_string(conn.connection.driver_connection)))

    def recreate_views(sql: typing.Any, conn: typing.Any, views: typing.List[typing.Dict[str, str]]) -> None:
        for view in views:
            if not view["is_materialized"]:
                logger.info("Recreating dummy view %s", view['fully_qualified_name'])
                query = sql.SQL('''
                    CREATE VIEW {fully_qualified_name} ({columns}) AS
                    SELECT {null_columns} FROM (SELECT 1) AS t LIMIT 0
                ''').format(
                    fully_qualified_name=sql.SQL(view['fully_qualified_name']),
                    columns=sql.SQL(view['column_names']),
                    null_columns=sql.SQL(view['null_typed_columns'])
                )
                conn.execute(sa.text(query.as_string(conn.connection.driver_connection)))

        for view in views:
            if view["is_materialized"]:
                logger.info("Recreating original materialized view %s", view['fully_qualified_name'])
                conn.execute(sa.text(view['materialized_definition']))
            else:
                logger.info("Recreating original view %s", view['fully_qualified_name'])
                logger.info(view['definition'])
                conn.execute(sa.text(view['definition']))

            if view['quoted_grantees']:
                logger.info('Granting SELECT on %s to %s', view['fully_qualified_name'], view['quoted_grantees'])
                conn.execute(sa.text(sql.SQL('GRANT SELECT ON {schema_table} TO {users}')
                    .format(
                        schema_table=sql.SQL(view['fully_qualified_name']),
                        users=sql.SQL(view['quoted_grantees']),
                    )
                    .as_string(conn.connection.driver_connection))
                )

    def create_first_batch_ingest_table_if_necessary(sql: typing.Any, conn: typing.Any, live_table: sa.Table, target_table: sa.Table) -> sa.Table:
        must_create_ingest_table = False

        if delete == Delete.BEFORE_FIRST_BATCH:
            must_create_ingest_table = True

        if not must_create_ingest_table:
            logger.info('Finding existing columns of %s.%s', str(target_table.schema), str(target_table.name))
            # Reflection can return a different server_default even if client code didn't set it
            for column in live_table.columns.values():
                column.server_default = None
            logger.info('Existing columns of %s.%s are %s', str(target_table.schema), str(target_table.name), list(live_table.columns))

            # postgresql_include should be ignored if empty, but it seems to "sometimes" appear
            # both in reflected tables, and when
            def get_dialect_kwargs(index: typing.Any) -> typing.Dict[str, typing.Any]:
                dialect_kwargs = dict(index.dialect_kwargs)
                if 'postgresql_include' in dialect_kwargs and not dialect_kwargs['postgresql_include']:
                    del dialect_kwargs['postgresql_include']
                return dialect_kwargs

            # Don't migrate if indexes only differ by name
            # Only way discovered is to temporarily change the name in each index name object
            @contextmanager
            def ignored_name(indexes: typing.Iterable[typing.Any]) -> typing.Generator[None, None, None]:
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

    def split_batch_into_tables(live_tables: typing.Any, combined_batch: typing.Any) -> typing.Generator[typing.Tuple[sa.Table, sa.Table, typing.Iterable[typing.Tuple[sa.Table, typing.Tuple[typing.Any]]]], None, None]:
        ingested_tables: typing.Set[typing.Optional[sa.Table]] = set()
        queues: typing.DefaultDict[typing.Any, typing.Deque[typing.Any]] = defaultdict(deque)
        current_queue = None
        current_table = None

        batch_iter = iter(batch)

        def next_skipping_tables_to_not_ingest(batch_iter):
            while True:
                table, row = next(batch_iter)
                if table not in live_tables:
                    continue
                return table, row

        def batch_for_current_table_until_a_queue_full() -> typing.Generator[typing.Any, None, None]:
            nonlocal current_queue, current_table

            while current_queue:
                table, row = current_queue.popleft()
                ingested_tables.add(table)
                yield row

            while True:
                try:
                    table, row = next_skipping_tables_to_not_ingest(batch_iter)
                except StopIteration:
                    current_queue = None
                    current_table = None
                    break

                if table is current_table:
                    ingested_tables.add(table)
                    yield row
                else:
                    queue = queues[table]
                    queue.append((table, row))
                    if len(queue) >= max_rows_per_table_buffer:
                        current_queue = queue
                        current_table = table
                        break

        # Bootstrap, choosing the first table from the first row
        try:
            table, row = next_skipping_tables_to_not_ingest(batch_iter)
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

    def insert_rows_psycopg2(sql: typing.Any, conn: typing.Any, user_facing_table: sa.Table, batch_table: sa.Table, rows: typing.Any) -> None:

        def get_converter(sa_type: typing.Any) -> typing.Callable[[typing.Any], str]:

            def escape_array_item(text: str) -> str:
                return text.replace('\\', '\\\\').replace('"', '\\"')

            def escape_string(text: str) -> str:
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
            elif isinstance(sa_type, BYTEA):
                return lambda v: (null if v is None else r'\\x' + v.hex().upper())
            else:
                return lambda v: (null if v is None else escape_string(str(v)))

        def logged(rows: typing.Iterable[typing.Any]) -> typing.Generator[typing.Any, None, None]:
            i = None
            logger.info("Ingesting from source into the database...")
            for i, row in enumerate(rows):
                yield row
                # Logs frequently for small numbers of rows, less frequently for larger
                if (i + 1) % (10 ** min((max((math.floor(math.log10(i + 1))), 2)), 6)) == 0:
                    logger.info("Ingested %s rows...", i + 1)

            total = i + 1 if i is not None else 0
            logger.info("Ingested %s rows in total", total)

        converters = tuple(get_converter(column.type) for column in batch_table.columns)
        db_rows = logged(
            '\t'.join(converter(value) for (converter,value) in zip(converters, row)) + '\n'
            for row in rows
        )
        with conn.connection.driver_connection.cursor() as cursor:
            cursor.copy_expert(str(bind_identifiers(sql, conn, "COPY {}.{} FROM STDIN", batch_table.schema, batch_table.name)), to_file_like_obj(db_rows, str), size=65536)

    def insert_rows_psycopg(sql: typing.Any, conn: typing.Any, user_facing_table: sa.Table, batch_table: sa.Table, rows: typing.Any) -> None:
        with \
                conn.connection.driver_connection.cursor() as cursor, \
                cursor.copy(str(bind_identifiers(sql, conn, "COPY {}.{} FROM STDIN", batch_table.schema, batch_table.name))) as copy:
            for row in rows:
                copy.write_row(row)

    sql, insert_rows = {
        **({'psycopg2': (sql2, insert_rows_psycopg2)} if sql2 is not None else {}),
        **({'psycopg': (sql3, insert_rows_psycopg)} if sql3 is not None else {}),
    }[conn.engine.driver]

    conn.begin()

    # Create the target tables without indexes, which are added later with
    # with logic to avoid duplicate names
    for target_table in metadata.tables.values():
        logger.info("Creating target table %s if it doesn't already exist", target_table)
        if target_table.schema not in sa.inspect(conn).get_schema_names():
            schema = sa.schema.CreateSchema(target_table.schema)
            conn.execute(schema)
        initial_table_metadata = sa.MetaData()
        sa.Table(
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

        batch_ingest_tables: typing.Dict[typing.Any, typing.Any] = {}

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
                        typing.cast(typing.List[typing.Any], tuple(col.name for col in columns_to_select)),
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
                insert_rows(sql, conn, target_table, ingest_table, table_batch)
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
                insert_rows(sql, conn, target_table, batch_table, table_batch)
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

            if ingest_table is not target_table:
                logger.info("Copying privileges for %s.%s", str(target_table.schema), str(target_table.name))
                grantees = conn.execute(sa.text(sql.SQL('''
                    SELECT acl.grantee::regrole AS quoted_grantee
                    FROM pg_class c, aclexplode(relacl) acl
                    WHERE c.oid = (quote_ident({schema}) || '.' || quote_ident({table}))::regclass
                    AND acl.privilege_type = 'SELECT'
                ''').format(schema=sql.Literal(target_table.schema), table=sql.Literal(target_table.name))
                    .as_string(conn.connection.driver_connection)
                )).fetchall()
                if grantees:
                    conn.execute(sa.text(sql.SQL('GRANT SELECT ON {schema_table} TO {users}')
                        .format(
                            schema_table=sql.Identifier(ingest_table.schema, ingest_table.name),
                            users=sql.SQL(',').join(
                                sql.SQL(grantee.quoted_grantee)
                                for grantee in grantees
                            ),
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
                    dependent_views = get_dependent_views(sql, conn, target_table.schema, target_table.name)
                    drop_views(sql, conn, dependent_views)
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
                    recreate_views(sql, conn, dependent_views)

        if callable(high_watermark_value):
            high_watermark_value = high_watermark_value()

        comment_parsed['pg-bulk-ingest'] = comment_parsed.get('pg-bulk-ingest', {})
        comment_parsed['pg-bulk-ingest']['high-watermark'] = high_watermark_value
        save_comment(sql, conn, target_tables[0].schema, target_tables[0].name, json.dumps(comment_parsed))

        conn.commit()
        logger.info('Ingestion of batch %s with high watermark value %s complete', batch_metadata, high_watermark_value)
        conn.begin()

    conn.commit()


_VIEWS_SQL = '''
-- A view is a row in pg_rewrite, and a corresponding set of rows in pg_depend where:
--
-- 1. There is a row per column that links pg_rewrite with all the columns of all the tables (or
--    other views) in pg_class that the view queries
-- 2. There is also one "internal" row linking the pg_rewrite row with its pg_class entry for the
--    view itself. This is can be used to then find the views that query the view, the fully
--    qualified name of the view, as well as its definition.
--
-- So to find all the direct views for a table, we find the rows in pg_depend for 1., and then self
-- join onto pg_depend again for 2. (making sure to not choose the same rows again). To then find
-- all the indirect views, we do the same thing repeatedly using a recursive CTE.
--
-- PostgreSQL does not forbid cycles of views, so during the recursion we have to make sure we
-- never add a view that has already been added.
WITH RECURSIVE view_deps AS (
    -- Non recursive term: direct views on the table
    SELECT
        view_depend.refobjid, array[view_depend.refobjid] as path
    FROM
        pg_depend as view_depend, pg_depend as table_depend
    WHERE
        -- Find rows in pg_rewrite for 1: rows that link the table with the views that query it
        table_depend.classid = 'pg_rewrite'::regclass
        and table_depend.refclassid = 'pg_class'::regclass
        and table_depend.refobjid = format('%I.%I', {schema}, {table})::regclass

        -- And we can find 2: the pg_class entry for each of the views themselves
        and view_depend.classid = 'pg_rewrite'::regclass
        and view_depend.refclassid = 'pg_class'::regclass
        and view_depend.deptype = 'i'
        and view_depend.objid = table_depend.objid
        and view_depend.refobjid != table_depend.refobjid

    UNION

    -- Recursive term: views on the views
    SELECT
        view_depend.refobjid, view_deps.path || array[view_depend.refobjid]
    FROM
        view_deps, pg_depend AS view_depend, pg_depend as table_depend
    WHERE
        -- Find rows in pg_rewrite for 1: rows that link the views with other views that query it
        table_depend.classid = 'pg_rewrite'::regclass
        and table_depend.refclassid = 'pg_class'::regclass
        and table_depend.refobjid = view_deps.refobjid

        -- And we can find 2: the pg_class entry for each of the next level views themselves
        and view_depend.classid = 'pg_rewrite'::regclass
        and view_depend.refclassid = 'pg_class'::regclass
        and view_depend.deptype = 'i'
        and view_depend.objid = table_depend.objid
        and view_depend.refobjid != table_depend.refobjid

        -- Making sure to not get into an infinite cycle
        and not view_depend.refobjid = ANY(view_deps.path)
)

-- We now get all the information in order to drop and re-create the views.
-- 
-- The ordering is important only for materialized views so they are re-created so all of their
-- upstream dependencies are re-created first. This is especially important because in the cases
-- where the same view appears in multiple places in the dependency graph, it is not in the order
-- in which the recursive CTE discovers them.
--
-- The ordering is by increasing max path length (depth) for any particular view from the table in
-- question, which ensures all its upstream views are earlier in the order. For the case of
-- materialized views, this is very almost ordering by pg_class.oid / view_deps.refobjid, because
-- _usually_ order of creation == order of view dependency == increasing order of oid. However,
-- oids can wrap around (after creating several billion tables...) so to handle this case we have
-- explicit ordering on the max path length. Unfortunately it is difficult to test this case, since
-- I don't think it's possible to choose oids in pg_class - you get what you're given.
--
-- For completeness, correct handling of non-materialized views doesn't depend on order because:
-- 1. We will drop using DROP IF EXISTS ... CASCADE
-- 2. We will create them first creating them all as "dummy" views with the right columns and
--    datatypes, and then replace them all with the correct definitions. This is the only way to
--    re-create cycles of views. These are admittedly not usable, but done on principle to leave
--    things as we found them, say for someone to then find out what queries the views use to then
--    manually fix the cycles. Note that even if we don't care about cycles or we had some
--    mechanism to forbid them, views on a table can be repeated at different levels in its
--    dependency graph, so this technique also allows us to not care about the order these are
--    created in
--
-- We also avoid more CTEs to allow older PostgreSQl to better optimise the following, but at the
-- cost of lower clarity due to having tables defined inline
--
-- Older PostgreSQL also doesn't allow a shorthand GROUP BY by primary key - if we coudl do this
-- we could join on pg_attribute and pg_class in the same SELECT statement and make the following
-- clearer I suspect
SELECT
    refobjid::regclass::text AS fully_qualified_name,
    column_names,
    null_typed_columns,
    pg_get_viewdef(refobjid) AS query,
    c.relkind = 'm' as is_materialized,
    (
        SELECT string_agg(grantee::regrole::text, ',')
        FROM aclexplode(coalesce(c.relacl, acldefault('r', c.relowner)))
        WHERE privilege_type = 'SELECT'
    ) AS quoted_grantees
FROM (
    SELECT
        view_deps_dedup.refobjid,
        max_path_length,
        string_agg(quote_ident(a.attname),
                   ', ' ORDER BY a.attnum) as column_names,
        string_agg('NULL::' || pg_catalog.format_type(a.atttypid, a.atttypmod),
                   ', ' ORDER BY a.attnum) as null_typed_columns
    FROM (
        SELECT DISTINCT ON (refobjid)
            view_deps.refobjid,
            array_length(view_deps.path, 1) AS max_path_length
        FROM
            view_deps
        ORDER BY
            1, 2 DESC
    ) AS view_deps_dedup
    INNER JOIN
        pg_attribute a
    ON
        a.attrelid = view_deps_dedup.refobjid
    WHERE
        a.attnum > 0 AND NOT a.attisdropped
    GROUP BY
        1, 2
) AS view_deps_columns_and_max_path_length
INNER JOIN
    pg_class c ON c.oid = refobjid
ORDER BY
    -- The _max_ here is for when a materialized view appears in mutiple places in the dependency
    -- graph, making sure that we put such views _after_ all of their upstream dependencies
    max_path_length,
    -- For consistency
    refobjid
'''

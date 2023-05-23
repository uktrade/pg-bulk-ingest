import uuid
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



class HighWatermark:
    LATEST = '__LATEST__'


class Delete:
    OFF = '__OFF__'
    ALL = '__ALL__'


class HighWatermark:
    LATEST = '__LATEST__'


class Visibility:
    AFTER_EACH_BATCH = '__AFTER_EACH_BATCH__'


def ingest(conn, metadata, batches,
           high_watermark=HighWatermark.LATEST, visibility=Visibility.AFTER_EACH_BATCH, delete=Delete.OFF,
           get_pg_force_execute=lambda conn: pg_force_execute(conn),
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
        live_table = sa.Table(target_table.name, sa.MetaData(), schema=target_table.schema, autoload_with=conn)
        live_table_column_names = set(live_table.columns.keys())

        columns_target = tuple(
            (col.name, repr(col.type), col.nullable, col.primary_key, col.index) for col in target_table.columns.values()
        )
        columns_live = tuple(
            (col.name, repr(col.type), col.nullable, col.primary_key, col.index) for col in live_table.columns.values()
        )
        columns_to_drop = tuple(col for col in columns_live if col not in columns_target)
        columns_to_add = tuple(col for col in columns_target if col not in columns_live)
        must_migrate = columns_target != columns_live

        columns_to_drop_obj = tuple(live_table.columns[col[0]] for col in columns_to_drop)
        columns_to_add_obj = tuple(target_table.columns[col[0]] for col in columns_to_add)
        migrate_in_place = must_migrate and \
            all(col.nullable for col in columns_to_add_obj) \
            and all(not col.primary_key for col in columns_to_drop_obj) \
            and all(not col.primary_key for col in columns_to_add_obj) \
            and all(not col.index for col in columns_to_drop_obj) \
            and all(not col.index for col in columns_to_add_obj) \
            and all(
                i == len(columns_target) - 1 or col not in columns_to_add or columns_target[i+1] in columns_to_add
                for i, col in enumerate(columns_target)
            ) # All columns to add are at the end

        via_migration_table = must_migrate and not migrate_in_place

        if migrate_in_place:
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
            with get_pg_force_execute(conn):
                conn.execute(sa.text(alter_query.as_string(conn.connection.driver_connection)))

            conn.commit()
            conn.begin()

        elif via_migration_table:
            migration_metadata = sa.MetaData()
            migration_table = sa.Table(
                uuid.uuid4().hex,
                migration_metadata,
                *(
                    sa.Column(column.name, column.type, primary_key=column.primary_key)
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

            with get_pg_force_execute(conn):
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

    # Create the target table
    target_table = next(iter(metadata.tables.values()))
    conn.execute(bind_identifiers(sql, conn, '''
        CREATE SCHEMA IF NOT EXISTS {}
    ''', target_table.schema))
    metadata.create_all(conn)

    is_upsert = any(column.primary_key for column in target_table.columns.values())

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

    high_watermark_value = \
        comment_parsed.get('pg-bulk-ingest', {}).get('high-watermark') if high_watermark == HighWatermark.LATEST else\
        high_watermark

    migrate_if_necessary(sql, conn, target_table, comment)

    if delete == Delete.ALL:
        conn.execute(sa.delete(target_table))

    for high_watermark_value, batch in batches(high_watermark_value):
        if not is_upsert:
            csv_copy(sql, copy_from_stdin, conn, target_table, target_table, batch)
        else:
            # Create a batch table, and ingest into it
            batch_metadata = sa.MetaData()
            batch_table = sa.Table(
                uuid.uuid4().hex,
                batch_metadata,
                *(
                    sa.Column(column.name, column.type, primary_key=column.primary_key)
                    for column in target_table.columns
                ),
                schema=target_table.schema
            )
            batch_metadata.create_all(conn)
            csv_copy(sql, copy_from_stdin, conn, target_table, batch_table, batch)

            # Copy from that batch table into the target table, using
            # ON CONFLICT to update any existing rows
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

            # Drop the batch table
            batch_metadata.drop_all(conn)

        comment_parsed['pg-bulk-ingest'] = comment_parsed.get('pg-bulk-ingest', {})
        comment_parsed['pg-bulk-ingest']['high-watermark'] = high_watermark_value
        save_comment(sql, conn, target_table.schema, target_table.name, json.dumps(comment_parsed))

        conn.commit()
        conn.begin()

    conn.commit()

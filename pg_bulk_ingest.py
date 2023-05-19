import uuid
from enum import Enum

import sqlalchemy as sa
import json
from io import IOBase


try:
    from psycopg2 import sql as sql2
except ImportError:
    sql2 = None

try:
    from psycopg import sql as sql3
except ImportError:
    sql3 = None


Mode = Enum('Mode', [
    'UPSERT_AND_COMMIT_EACH_BATCH',
    'DELETE_ALL_ROWS_THEN_UPSERT_AND_COMMIT_EACH_BATCH',
])


def ingest(conn, metadata, batches, mode=Mode.UPSERT_AND_COMMIT_EACH_BATCH):

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

    def csv_copy(sql, copy_from_stdin, conn, user_facing_table, intermediate_table, rows):

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

        converters = tuple(get_converter(column.type) for column in intermediate_table.columns)
        db_rows = (
            '\t'.join(converter(value) for (converter,value) in zip(converters, row)) + '\n'
            for row, row_table in rows
            if row_table is user_facing_table
        )
        with conn.connection.driver_connection.cursor() as cursor:
            copy_from_stdin(cursor, str(bind_identifiers(sql, conn, "COPY {}.{} FROM STDIN", intermediate_table.schema, intermediate_table.name)), to_file_like_obj(db_rows, str))

    sql, copy_from_stdin = sql_and_copy_from_stdin(conn.engine.driver)

    # Create the target table
    for table in metadata.tables.values():
        conn.execute(bind_identifiers(sql, conn, '''
            CREATE SCHEMA IF NOT EXISTS {}
        ''', table.schema))
    metadata.create_all(conn)

    first_table = next(iter(metadata.tables.values()))
    live_table = sa.Table(first_table.name, sa.MetaData(), schema=first_table.schema, autoload_with=conn)
    live_table_column_names = set(live_table.columns.keys())

    if mode is Mode.DELETE_ALL_ROWS_THEN_UPSERT_AND_COMMIT_EACH_BATCH:
        conn.execute(sa.delete(first_table))

    # Add missing columns
    for column_name, column in first_table.columns.items():
        if column_name not in live_table_column_names:
            alter_query = sql.SQL('''
                ALTER TABLE {schema}.{table}
                ADD COLUMN {column_name} {column_type}
            ''').format(
                schema=sql.Identifier(first_table.schema),
                table=sql.Identifier(first_table.name),
                column_name=sql.Identifier(column_name),
                column_type=sql.SQL(column.type.compile(conn.engine.dialect)),
            )
            conn.execute(sa.text(alter_query.as_string(conn.connection.driver_connection)))

    is_upsert = \
        mode is Mode.UPSERT_AND_COMMIT_EACH_BATCH \
        and any(column.primary_key for column in first_table.columns.values())

    for batch in batches:
        if not is_upsert:
            csv_copy(sql, copy_from_stdin, conn, first_table, first_table, batch)
        else:
            # Create the intermediate tables, and ingest into them
            intermediate_metadata = sa.MetaData()
            intermediate_tables = tuple(sa.Table(
                uuid.uuid4().hex,
                intermediate_metadata,
                *(
                    sa.Column(column.name, column.type, primary_key=column.primary_key)
                    for column in table.columns
                ),
                schema=table.schema
            ) for table in tuple(metadata.tables.values()))
            intermediate_metadata.create_all(conn)
            first_intermediate_table = intermediate_tables[0]
            csv_copy(sql, copy_from_stdin, conn, first_table, first_intermediate_table, batch)

            # Copy from that intermediate table into the main table, using
            # ON CONFLICT to update any existing rows
            insert_query = sql.SQL('''
                INSERT INTO {schema}.{table}
                SELECT DISTINCT ON({primary_keys}) * FROM {intermediate_schema}.{intermediate_table}
                ON CONFLICT({primary_keys})
                DO UPDATE SET {updates}
            ''').format(
                schema=sql.Identifier(first_table.schema),
                table=sql.Identifier(first_table.name),
                intermediate_schema=sql.Identifier(first_intermediate_table.schema),
                intermediate_table=sql.Identifier(first_intermediate_table.name),
                primary_keys=sql.SQL(',').join((sql.Identifier(column.name) for column in first_table.columns if column.primary_key)),
                updates=sql.SQL(',').join(sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(column.name), sql.Identifier(column.name)) for column in first_table.columns),
            )
            conn.execute(sa.text(insert_query.as_string(conn.connection.driver_connection)))

            # Drop the intermediate table
            intermediate_metadata.drop_all(conn)

        conn.commit()
        conn.begin()

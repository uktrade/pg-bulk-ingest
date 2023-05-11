import uuid

import sqlalchemy as sa
import json
from psycopg2 import sql
from io import IOBase


def upsert(conn, metadata, rows):

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

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

    def get_converter(sa_type):
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

    first_table = next(iter(metadata.tables.values()))
    intermediate_tables = tuple(sa.Table(
        uuid.uuid4().hex,
        metadata,
        *(
            sa.Column(column.name, column.type, primary_key=column.primary_key)
            for column in table.columns
        ),
        schema=table.schema
    ) for table in tuple(metadata.tables.values()))

    # Create the table and intermediate tables
    for intermediate_table in intermediate_tables:
        conn.execute(bind_identifiers('''
            CREATE SCHEMA IF NOT EXISTS {}
        ''', intermediate_table.schema))
    metadata.create_all(conn)

    # Insert rows into just the first intermediate table
    first_intermediate_table = intermediate_tables[0]
    converters = tuple(get_converter(column.type) for column in first_intermediate_table.columns)
    def db_rows():
        for row, table in rows:
            if table is not first_table:
                continue
            yield '\t'.join(converter(value) for (converter,value) in zip(converters, row)) + '\n'

    with conn.connection.driver_connection.cursor() as cursor:
        cursor.copy_expert(str(bind_identifiers("COPY {}.{} FROM STDIN", first_intermediate_table.schema, first_intermediate_table.name)), to_file_like_obj(db_rows(), str), size=65536)


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
    metadata.drop_all(conn, tables=intermediate_tables)

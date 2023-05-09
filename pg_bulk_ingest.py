import uuid

import sqlalchemy as sa
from psycopg import sql


def upsert(conn, metadata, rows):

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

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
    with \
            conn.connection.driver_connection.cursor() as cursor, \
            cursor.copy(str(bind_identifiers("COPY {}.{} FROM STDIN", first_intermediate_table.schema, first_intermediate_table.name))) as copy:

        for row, table in rows:
            if table is not first_table:
                continue
            copy.write_row(row)

    # Copy from that intermediate table into the main table, using
    # ON CONFLICT to update any existing rows
    primary_keys = tuple(column.name for column in first_table.columns if column.primary_key)
    primary_keys_fragment = sql.SQL(',').join((sql.Identifier(primary_key) for primary_key in primary_keys)) 
    insert_fragment = sql.SQL('INSERT INTO {}.{}').format(sql.Identifier(first_table.schema), sql.Identifier(first_table.name))
    select_fragment = sql.SQL('SELECT DISTINCT ON({}) * FROM {}.{}').format(primary_keys_fragment, sql.Identifier(first_intermediate_table.schema), sql.Identifier(first_intermediate_table.name))
    on_conflict_fragment = sql.SQL('ON CONFLICT(') + primary_keys_fragment + sql.SQL(')')
    do_update_fragment = sql.SQL('DO UPDATE SET') + sql.SQL(',').join(sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(column.name), sql.Identifier(column.name)) for column in first_table.columns)
    insert_query = insert_fragment + select_fragment + on_conflict_fragment + do_update_fragment

    conn.execute(sa.text(insert_query.as_string(conn.connection.driver_connection)))

    # Drop the intermediate table
    metadata.drop_all(conn, tables=intermediate_tables)

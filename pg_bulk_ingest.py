import uuid

import sqlalchemy as sa
from psycopg import sql


def upsert(conn, table, rows):

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    intermediate_table_name = uuid.uuid4().hex
    intermediate_table = sa.Table(
        intermediate_table_name,
        table.metadata,
        *(
            sa.Column(column.name, column.type, primary_key=column.primary_key)
            for column in table.columns
        ),
        schema=table.schema
    )

    # Create an intermediate table like table
    conn.execute(bind_identifiers('''
        CREATE SCHEMA IF NOT EXISTS {}
    ''', table.schema))
    table.metadata.create_all(conn, tables=(intermediate_table,))

    # Insert rows into that intermediate table

    # Copy from that intermediate table into the main table, using
    # ON CONFLICT to update any existing rows

    # Drop the intermediate table
    table.metadata.drop_all(conn, tables=(intermediate_table,))

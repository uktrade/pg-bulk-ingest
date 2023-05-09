import uuid

import sqlalchemy as sa
from psycopg import sql


def upsert(conn, table, rows):

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = uuid.uuid4().hex

    # Create an intermediate table like table
    conn.execute(bind_identifiers('''
        CREATE SCHEMA IF NOT EXISTS {}
    ''', table.schema))
    conn.execute(bind_identifiers('''
        CREATE TABLE {}.{}(id int)
    ''', table.schema, table_name))

    # Insert rows into that intermediate table

    # Copy from that intermediate table into the main table, using
    # ON CONFLICT to update any existing rows

    # Drop the intermediate table
    conn.execute(bind_identifiers('''
        DROP TABLE {}.{}
    ''', table.schema, table_name))

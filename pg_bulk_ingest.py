import uuid

import sqlalchemy as sa
from psycopg import sql


def upsert(conn, table, rows):
    driver_connection = conn.connection.driver_connection
    table_name = uuid.uuid4().hex

    # Create an intermediate table like table
    query = sa.text(sql.SQL('''
        CREATE SCHEMA IF NOT EXISTS {}
    ''').format(sql.Identifier(table.schema)).as_string(driver_connection))
    conn.execute(query)
    query = sa.text(sql.SQL('''
        CREATE TABLE {}.{}(id int)
    ''').format(sql.Identifier(table.schema), sql.Identifier(table_name)).as_string(driver_connection))
    conn.execute(query)

    # Insert rows into that intermediate table

    # Copy from that intermediate table into the main table, using
    # ON CONFLICT to update any existing rows

    # Drop the intermediate table
    query = sa.text(sql.SQL('''
        DROP TABLE {}.{}
    ''').format(sql.Identifier(table.schema), sql.Identifier(table_name)).as_string(driver_connection))
    conn.execute(query)

import uuid

import sqlalchemy as sa
from psycopg import sql


def upsert(conn, metadata, rows):

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    intermediate_tables = tuple(sa.Table(
        uuid.uuid4().hex,
        metadata,
        *(
            sa.Column(column.name, column.type, primary_key=column.primary_key)
            for column in table.columns
        ),
        schema=table.schema
    ) for table in tuple(metadata.tables.values()))

    # Create the intermediate tables
    for intermediate_table in intermediate_tables:
        conn.execute(bind_identifiers('''
            CREATE SCHEMA IF NOT EXISTS {}
        ''', intermediate_table.schema))
    metadata.create_all(conn, tables=intermediate_tables)

    # Insert rows into just the first intermediate table
    for row, table in rows:
        pass

    # Copy from that intermediate table into the main table, using
    # ON CONFLICT to update any existing rows

    # Drop the intermediate table
    metadata.drop_all(conn, tables=intermediate_tables)

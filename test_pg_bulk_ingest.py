import uuid

from psycopg2 import sql
import sqlalchemy as sa

from pg_bulk_ingest import upsert


def test():

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine('postgresql+psycopg2://postgres@127.0.0.1:5432/')

    rows = (
        (3, 'd'),
        (4, 'a'),
        (5, 'q'),
    )

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        schema="my_schema_other",
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id', table_name)).fetchall()

    assert results == [(3, 'd'), (4, 'a'), (5, 'q')]

    rows = (
        (5, 'X'),
        (6, 'a'),
        (7, 'q'),
    )

    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id', table_name)).fetchall()

    assert results == [(3, 'd'), (4, 'a'), (5, 'X'), (6, 'a'), (7, 'q')]

import uuid
from datetime import date

import sqlalchemy as sa

try:
    # psycopg3
    from psycopg import sql
    engine_type = 'postgresql+psycopg'
except ImportError:
    # psycopg2
    from psycopg2 import sql
    engine_type = 'postgresql+psycopg2'

from pg_bulk_ingest import upsert


def test():

    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    rows = (
        (3, 'd', date(2023, 1, 1)),
        (4, 'a', date(2023, 1, 2)),
        (5, 'q', date(2023, 1, 3)),
    )

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        schema="my_schema_other",
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id', table_name)).fetchall()

    assert results == [(3, 'd', date(2023, 1, 1)), (4, 'a', date(2023, 1, 2)), (5, 'q', date(2023, 1, 3))]

    rows = (
        (5, 'X', date(2023, 1, 4)),
        (6, 'a', date(2023, 1, 5)),
        (7, 'q', date(2023, 1, 6)),
    )

    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id', table_name)).fetchall()

    assert results == [
        (3, 'd', date(2023, 1, 1)), 
        (4, 'a', date(2023, 1, 2)),         
        (5, 'X', date(2023, 1, 4)),
        (6, 'a', date(2023, 1, 5)),
        (7, 'q', date(2023, 1, 6)),
    ]

    assert len(metadata_obj.tables) == 1

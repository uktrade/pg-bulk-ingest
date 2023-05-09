import sqlalchemy as sa

from pg_bulk_ingest import upsert


def test():
    engine = sa.create_engine('postgresql+psycopg://postgres@127.0.0.1:5432/', echo=True)

    rows = (
        (3, 'd'),
        (4, 'a'),
        (5, 'q'),
    )

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        "my_table",
        metadata_obj,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        schema="my_schema_other",
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in rows))

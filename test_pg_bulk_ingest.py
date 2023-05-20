import uuid
from datetime import date

import sqlalchemy as sa

try:
    # psycopg2
    from psycopg2 import sql
    engine_type = 'postgresql+psycopg2'
except ImportError:
    # psycopg3
    from psycopg import sql
    engine_type = 'postgresql+psycopg'

engine_future = {'future': True} if tuple(int(v) for v in sa.__version__.split('.')) < (2, 0, 0) else {}

from pg_bulk_ingest import Delete, ingest


def test_upsert():
    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (
            ((3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}), my_table),
        ),
        (
            ((4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}), my_table),
            ((5, 6, 'q', None, [1,2], {}, {}), my_table)
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj, initial_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}), my_table),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}), my_table),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}), my_table),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj, updated_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8, 'q', date(2023, 1, 6), [1,2], {}, {}),
    ]

    assert len(metadata_obj.tables) == 1

def test_upsert_extra_column():
    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_obj_1 = sa.MetaData()
    my_table_1 = sa.Table(
        table_name,
        metadata_obj_1,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (
            ((3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}), my_table_1),
        ),
        (
            ((4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}), my_table_1),
            ((5, 6, 'q', None, [1,2], {}, {}), my_table_1)
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_1, initial_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}), my_table_1),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}), my_table_1),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}), my_table_1),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_1, updated_rows)

    with engine.begin() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id_1', 'id_2')).fetchall()

    metadata_obj_2 = sa.MetaData()
    my_table_2 = sa.Table(
        table_name,
        metadata_obj_2,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        sa.Column("nullable_string", sa.VARCHAR(16), nullable=True),
        schema="my_schema_other",
    )

    updated_rows_new_column = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}, 'abc'), my_table_2),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}, 'def'), my_table_2),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}, 'ghi'), my_table_2),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_2, updated_rows_new_column)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}, None),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}, None),
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}, 'abc'),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}, 'def'),
        (7, 8, 'q', date(2023, 1, 6), [1,2], {}, {}, 'ghi'),
    ]

    assert len(metadata_obj_1.tables) == 1


def test_upsert_extra_column_not_at_end():
    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_obj_1 = sa.MetaData()
    my_table_1 = sa.Table(
        table_name,
        metadata_obj_1,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (
            ((3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}), my_table_1),
        ),
        (
            ((4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}), my_table_1),
            ((5, 6, 'q', None, [1,2], {}, {}), my_table_1)
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_1, initial_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}), my_table_1),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}), my_table_1),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}), my_table_1),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_1, updated_rows)

    with engine.begin() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id_1', 'id_2')).fetchall()

    metadata_obj_2 = sa.MetaData()
    my_table_2 = sa.Table(
        table_name,
        metadata_obj_2,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("nullable_string", sa.VARCHAR(16), nullable=True),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )

    updated_rows_new_column = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, 'abc', {}), my_table_2),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, 'def', {}), my_table_2),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, 'ghi', {}), my_table_2),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_2, updated_rows_new_column)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, None, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, None, {}),
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, 'abc', {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, 'def', {}),
        (7, 8, 'q', date(2023, 1, 6), [1,2], {}, 'ghi', {}),
    ]

    assert len(metadata_obj_1.tables) == 1


def test_migrate_no_data():
    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_obj_1 = sa.MetaData()
    my_table_1 = sa.Table(
        table_name,
        metadata_obj_1,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (
            ((3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}), my_table_1),
        ),
        (
            ((4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}), my_table_1),
            ((5, 6, 'q', None, [1,2], {}, {}), my_table_1)
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj_1, initial_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id_1', 'id_2')).fetchall()

    metadata_obj_2 = sa.MetaData()
    my_table_2 = sa.Table(
        table_name,
        metadata_obj_2,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("nullable_string", sa.VARCHAR(16), nullable=True),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )

    updated_rows_new_column = ()
    with engine.connect() as conn:
        ingest(conn, metadata_obj_2, updated_rows_new_column)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, None, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, None, {}),
        (5, 6, 'q', None, [1,2], {}, None, {}),
    ]

    assert len(metadata_obj_1.tables) == 1

def test_insert():
    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id_1", sa.INTEGER),
        sa.Column("id_2", sa.INTEGER),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (
            ((3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}), my_table),
        ),
        (
            ((4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}), my_table),
            ((5, 6, 'q', None, [1,2], {}, {}), my_table),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj, initial_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}), my_table),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}), my_table),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}), my_table),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj, updated_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2', 'value')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8, 'q', date(2023, 1, 6), [1,2], {}, {}),
    ]

    assert len(metadata_obj.tables) == 1


def test_replace():
    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value", sa.VARCHAR(16), nullable=False),
        sa.Column("date", sa.DATE, nullable=True),
        sa.Column("array", sa.ARRAY(sa.INTEGER), nullable=False),
        sa.Column("json", sa.dialects.postgresql.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (
            ((3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}), my_table),
        ),
        (
            ((4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}), my_table),
            ((5, 6, 'q', None, [1,2], {}, {}), my_table),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj, initial_rows)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2')).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (
            ((5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}), my_table),
            ((6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}), my_table),
        ),
        (
            ((7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}), my_table),
        )
    )
    with engine.connect() as conn:
        ingest(conn, metadata_obj, updated_rows, delete=Delete.ALL)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2')).fetchall()

    assert tuple(results) == (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}),
    )

    assert len(metadata_obj.tables) == 1


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

from pg_bulk_ingest import insert, upsert, replace


def test_upsert():
    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id_1", sa.Integer, primary_key=True),
        sa.Column("id_2", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        sa.Column("date", sa.Date, nullable=True),
        sa.Column("array", sa.ARRAY(sa.Integer), nullable=False),
        sa.Column("json", sa.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in initial_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}),
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj, ((row, my_table) for row in updated_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8, 'q', date(2023, 1, 6), [1,2], {}, {}),
    ]

    assert len(metadata_obj.tables) == 1

def test_upsert_extra_column():
    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    metadata_obj_1 = sa.MetaData()
    my_table_1 = sa.Table(
        table_name,
        metadata_obj_1,
        sa.Column("id_1", sa.Integer, primary_key=True),
        sa.Column("id_2", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        sa.Column("date", sa.Date, nullable=True),
        sa.Column("array", sa.ARRAY(sa.Integer), nullable=False),
        sa.Column("json", sa.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj_1, ((row, my_table_1) for row in initial_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}),
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj_1, ((row, my_table_1) for row in updated_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    metadata_obj_2 = sa.MetaData()
    my_table_2 = sa.Table(
        table_name,
        metadata_obj_2,
        sa.Column("id_1", sa.Integer, primary_key=True),
        sa.Column("id_2", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        sa.Column("date", sa.Date, nullable=True),
        sa.Column("array", sa.ARRAY(sa.Integer), nullable=False),
        sa.Column("json", sa.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        sa.Column("nullable_string", sa.String(16), nullable=True),
        schema="my_schema_other",
    )

    updated_rows_new_column = (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}, 'abc'),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}, 'def'),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}, 'ghi'),
    )
    with engine.begin() as conn:
        upsert(conn, metadata_obj_2, ((row, my_table_2) for row in updated_rows_new_column))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}, None),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}, None),
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}, 'abc'),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}, 'def'),
        (7, 8, 'q', date(2023, 1, 6), [1,2], {}, {}, 'ghi'),
    ]

    assert len(metadata_obj_1.tables) == 1

def test_insert():
    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id_1", sa.Integer),
        sa.Column("id_2", sa.Integer),
        sa.Column("value", sa.String(16), nullable=False),
        sa.Column("date", sa.Date, nullable=True),
        sa.Column("array", sa.ARRAY(sa.Integer), nullable=False),
        sa.Column("json", sa.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    )
    with engine.begin() as conn:
        insert(conn, metadata_obj, ((row, my_table) for row in initial_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}),
    )
    with engine.begin() as conn:
        insert(conn, metadata_obj, ((row, my_table) for row in updated_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2, value', table_name)).fetchall()

    assert tuple(results) == initial_rows + updated_rows

    assert len(metadata_obj.tables) == 1


def test_replace():
    def bind_identifiers(query_str, *identifiers):
        return sa.text(sql.SQL(query_str).format(
            *(sql.Identifier(identifier) for identifier in identifiers)
        ).as_string(conn.connection.driver_connection))

    table_name = "my_table_" + uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    metadata_obj = sa.MetaData()
    my_table = sa.Table(
        table_name,
        metadata_obj,
        sa.Column("id_1", sa.Integer, primary_key=True),
        sa.Column("id_2", sa.Integer, primary_key=True),
        sa.Column("value", sa.String(16), nullable=False),
        sa.Column("date", sa.Date, nullable=True),
        sa.Column("array", sa.ARRAY(sa.Integer), nullable=False),
        sa.Column("json", sa.JSON, nullable=False),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB, nullable=False),
        schema="my_schema_other",
    )
    initial_rows = (
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    )
    with engine.begin() as conn:
        replace(conn, metadata_obj, ((row, my_table) for row in initial_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert results == [
        (3, 4, 'd', date(2023, 1, 1), [1,2], {'a': 2}, {'c': None}),
        (4, 5, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 6, 'q', None, [1,2], {}, {}),
    ]

    updated_rows = (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}),
    )
    with engine.begin() as conn:
        replace(conn, metadata_obj, ((row, my_table) for row in updated_rows))

    with engine.begin() as conn:
        results = conn.execute(bind_identifiers('SELECT * FROM my_schema_other.{} ORDER BY id_1, id_2', table_name)).fetchall()

    assert tuple(results) == (
        (5, 6, 'X', date(2023, 1, 4), [1,2], {}, {}),
        (6, 7, 'a', date(2023, 1, 5), [1,2], {'b': 3}, {}),
        (7, 8 ,'q', date(2023, 1, 6), [1,2], {}, {}),
    )

    assert len(metadata_obj.tables) == 1


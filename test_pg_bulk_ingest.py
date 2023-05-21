import uuid
from datetime import date

import pytest
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


def test_data_types():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER),
        sa.Column("varchar", sa.VARCHAR),
        sa.Column("date", sa.DATE),
        sa.Column("array", sa.ARRAY(sa.INTEGER)),
        sa.Column("json", sa.dialects.postgresql.JSON),
        sa.Column("jsonb", sa.dialects.postgresql.JSONB),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None,
            (
                (my_table, (4, 'a', date(2023, 1, 2), [1,2], {}, {})),
                (my_table, (5, 'b', None, [1,2], {}, {})),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (4, 'a', date(2023, 1, 2), [1,2], {}, {}),
        (5, 'b', None, [1,2], {}, {}),
    ]


def test_batches():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None,
            (
                (my_table, (1,)),
            ),
        ),
        (
            None,
            (
                (my_table, (2,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (1,),
        (2,),
    ]


def test_batch_visible_only_after_batch_complete():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER),
        schema="my_schema",
    )

    table_not_found_before_batch_1 = False

    results_after_batch_1 = None

    batch_2_result_1 = None
    batch_2_result_2 = None
    batch_2_result_3 = None
    results_after_batch_2 = None

    def batch_1():
        yield (my_table, (1,))
        yield (my_table, (2,))

    def batch_2():
        nonlocal batch_2_result_1, batch_2_result_2, batch_2_result_3

        with engine.connect() as conn:
            batch_2_result_1 = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

        yield (my_table, (3,))

        with engine.connect() as conn:
            batch_2_result_2 = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

        yield (my_table, (4,))

        with engine.connect() as conn:
            batch_2_result_3 = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    def batches(_):
        nonlocal table_not_found_before_batch_1, results_after_batch_1, results_after_batch_2
        with engine.connect() as conn:
            try:
                conn.execute(sa.select(my_table).order_by('integer')).fetchall()
            except sa.exc.ProgrammingError:
                table_not_found_before_batch_1 = True

        yield None, batch_1()

        with engine.connect() as conn:
            results_after_batch_1 = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

        yield None, batch_2()
        with engine.connect() as conn:
            results_after_batch_2 = conn.execute(sa.select(my_table).order_by('integer')).fetchall()
  
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert table_not_found_before_batch_1 == True
    assert results_after_batch_1 == [
        (1,),
        (2,),
    ]

    assert batch_2_result_1 == [
        (1,),
        (2,),
    ]
    assert batch_2_result_2 == [
        (1,),
        (2,),
    ]
    assert batch_2_result_3 == [
        (1,),
        (2,),
    ]
    assert results_after_batch_2 == [
        (1,),
        (2,),
        (3,),
        (4,),
    ]


def test_upsert():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "mY_t\"\'able_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id\"\'_2🍰", sa.INTEGER, primary_key=True),
        sa.Column("Value_1🍰", sa.VARCHAR),
        sa.Column("va\"'lue_2", sa.VARCHAR),
        schema="my_Sche\"ma\'🍰",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table, (1, 2, 'a', 'b')),
                (my_table, (3, 4, 'c', 'd')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    batches_2 = lambda _: (
        (
            None,
            (
                (my_table, (3, 4, 'e', 'f')),
                (my_table, (3, 6, 'g', 'h')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_2
            )

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id\"\'_2🍰')).fetchall()

    assert results == [
        (1, 2, 'a', 'b'),
        (3, 4, 'e', 'f'),
        (3, 6, 'g', 'h'),
    ]

    assert len(metadata.tables) == 1


def test_high_watermark_is_preserved_between_ingests():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        sa.Column("value_2", sa.VARCHAR),
        schema="my_schema",
    )

    high_watermarks = []

    def batches(high_watermark):
        high_watermarks.append(high_watermark)
        yield (high_watermark or 0) + 1, ()
        yield (high_watermark or 0) + 2, ()
        yield (high_watermark or 0) + 3, ()

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None]

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3]


    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3, 6]


def test_high_watermark_is_preserved_between_ingests_no_primary_key():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER),
        sa.Column("id_2", sa.INTEGER),
        sa.Column("value_1", sa.VARCHAR),
        sa.Column("value_2", sa.VARCHAR),
        schema="my_schema",
    )

    high_watermarks = []

    def batches(high_watermark):
        high_watermarks.append(high_watermark)
        yield (high_watermark or 0) + 1, ()
        yield (high_watermark or 0) + 2, ()
        yield (high_watermark or 0) + 3, ()

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None]

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3]


    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3, 6]


def test_high_watermark_is_preserved_if_exception():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        sa.Column("value_2", sa.VARCHAR),
        schema="my_schema",
    )

    high_watermarks = []

    def batches(high_watermark):
        high_watermarks.append(high_watermark)
        yield (high_watermark or 0) + 1, ()
        if (high_watermark or 0) > 4:
            raise Exception()
        yield (high_watermark or 0) + 3, ()

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None]

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3]

    with \
            pytest.raises(Exception), \
            engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3, 6]

    with \
            pytest.raises(Exception), \
            engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, 3, 6, 7]


def test_high_watermark_is_passed_into_the_batch_function():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        sa.Column("value_2", sa.VARCHAR),
        schema="my_schema",
    )

    high_watermarks = []

    def batches(high_watermark):
        high_watermarks.append(high_watermark)
        yield (high_watermark or 0) + 1, ()
        yield (high_watermark or 0) + 2, ()
        yield (high_watermark or 0) + 3, ()

    with engine.connect() as conn:
        ingest(conn, metadata, batches, high_watermark=10)

    assert high_watermarks == [10]


def test_migrate_add_column_at_end():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        schema="my_schema",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table_1, (1, 'a')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        sa.Column("value_2", sa.VARCHAR),
        schema="my_schema",
    )
    batch_2_result = None
    def batches_2(_):
        nonlocal batch_2_result
        with engine.connect() as conn:
            batch_2_result = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

        yield None, (
            (my_table_2, (2, 'b', 'c')),
        )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    assert batch_2_result == [
        (1, 'a', None),
    ]
    assert results == [
        (1, 'a', None),
        (2, 'b', 'c'),
    ]

    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_migrate_add_column_not_at_end():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_a", sa.VARCHAR),
        sa.Column("value_b", sa.VARCHAR),
        schema="my_schema",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table_1, (1, 'a', 'b')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_a", sa.VARCHAR),
        sa.Column("value_c", sa.VARCHAR),
        sa.Column("value_b", sa.VARCHAR),
        schema="my_schema",
    )
    batch_2_result = None
    def batches_2(_):
        nonlocal batch_2_result
        with engine.connect() as conn:
            batch_2_result = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

        yield None, (
            (my_table_2, (2, 'a', 'c', 'b')),
        )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    assert batch_2_result == [
        (1, 'a', None, 'b'),
    ]
    assert results == [
        (1, 'a', None, 'b'),
        (2, 'a', 'c', 'b'),
    ]

    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_migrate_add_column_not_at_end_no_data():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_a", sa.VARCHAR),
        sa.Column("value_b", sa.VARCHAR),
        schema="my_schema",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table_1, (1, 'a', 'b')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_a", sa.VARCHAR),
        sa.Column("value_c", sa.VARCHAR),
        sa.Column("value_b", sa.VARCHAR),
        schema="my_schema",
    )
    batches_2 = lambda _: (
    )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    assert results == [
        (1, 'a', None, 'b'),
    ]

    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_migrate_add_column_not_at_end_no_data():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_a", sa.VARCHAR),
        sa.Column("value_b", sa.VARCHAR),
        schema="my_schema",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table_1, (1, 'a', 'b')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_a", sa.VARCHAR),
        sa.Column("value_c", sa.VARCHAR),
        sa.Column("value_b", sa.VARCHAR),
        schema="my_schema",
    )
    batches_2 = lambda _: (
    )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    assert results == [
        (1, 'a', None, 'b'),
    ]

    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_insert():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER),
        schema="my_schema",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table, (1,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    batches_2 = lambda _: (
        (
            None,
            (
                (my_table, (2,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (1,),
        (2,),
    ]

    assert len(metadata.tables) == 1


def test_delete():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER),
        schema="my_schema",
    )
    batches_1 = lambda _: (
        (
            None,
            (
                (my_table, (1,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    batches_2 = lambda _: (
        (
            None,
            (
                (my_table, (2,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_2, delete=Delete.ALL)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (2,),
    ]

    assert len(metadata.tables) == 1

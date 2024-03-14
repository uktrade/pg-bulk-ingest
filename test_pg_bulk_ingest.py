import uuid
import io
import itertools
from contextlib import contextmanager
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

from pg_bulk_ingest import Delete, ingest, HighWatermark, Upsert, to_file_like_obj


def _get_table_oid(engine, table):
    with engine.connect() as conn:
        return conn.execute(sa.text(sql.SQL('''
             SELECT (quote_ident({schema}) || '.' || quote_ident({table}))::regclass::oid
        ''').format(
            schema=sql.Literal(table.schema),
            table=sql.Literal(table.name),
        ).as_string(conn.connection.driver_connection))).fetchall()[0][0]


def _no_batches(_):
    yield from ()


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
        sa.Column("bytes", sa.dialects.postgresql.BYTEA),
        schema="my_schema",
    )

    batches = lambda _: (
        (
            None, None,
            (
                (my_table, (4, 'a', date(2023, 1, 2), [1,2], {}, {}, b'\x80')),
                (my_table, (5, 'b', None, [1,2], {}, {}, b'\x00')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()
    
    assert results == [
        (4, 'a', date(2023, 1, 2), [1,2], {}, {}, b'\x80'),
        (5, 'b', None, [1,2], {}, {}, b'\x00'),
    ]


def test_large_amounts_of_data():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("varchar", sa.VARCHAR),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (
                (my_table, ('a' * 1000,))
                for _ in range(0, 10000)
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    with engine.connect() as conn:
        results = conn.execution_options(stream_results=True).execute(sa.select(my_table))
        total_length = sum(len(r[0]) for r in results)

    assert total_length == 1000 * 10000


def test_unique_initial():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER, unique=True),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (
                (my_table, (1,)),
                (my_table, (1,)),
            ),
        ),
    )
    with pytest.raises(Exception, match='violates unique constraint'):
        with engine.connect() as conn:
            ingest(conn, metadata, batches)


def test_unique_added():
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
            None, None,
            (
                (my_table, (1,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    metadata = sa.MetaData()
    my_table_with_unique = sa.Table(
        my_table.name,
        metadata,
        sa.Column("integer", sa.INTEGER, unique=True),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (
                (my_table_with_unique, (1,)),
            ),
        ),
    )
    with pytest.raises(Exception, match='violates unique constraint'):
        with engine.connect() as conn:
            ingest(conn, metadata, batches)


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
            None, None,
            (
                (my_table, (1,)),
            ),
        ),
        (
            None, None,
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

def test_if_no_batches_then_only_target_table_visible():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)
    with engine.connect() as conn:
        first_check = conn.execute(sa.text('''
            SELECT count(*) FROM pg_class;
        ''')).fetchall()[0][0]

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("integer", sa.INTEGER),
        schema="my_schema",
    )
    batches = lambda _: []

    with engine.connect() as conn:
        ingest(conn, metadata, batches, delete=Delete.BEFORE_FIRST_BATCH)

    with engine.connect() as conn:
        number_of_rows = len(conn.execute(sa.select(my_table)).fetchall())

        last_check = conn.execute(sa.text('''
            SELECT count(*) FROM pg_class;
        ''')).fetchall()[0][0]

    # The table must exist to have gotten this
    assert number_of_rows == 0

    # we expect the target table to be visible but no others
    assert last_check == first_check + 1


def test_batches_with_long_index_name():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("very_long_column_name_even_longer", sa.INTEGER, index=True),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (
                (my_table, (1,)),
            ),
        ),
        (
            None, None,
            (
                (my_table, (2,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('very_long_column_name_even_longer')).fetchall()

    assert results == [
        (1,),
        (2,),
    ]

    oid_1 = _get_table_oid(engine, my_table)

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    oid_2 = _get_table_oid(engine, my_table)

    assert oid_1 == oid_2


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

        yield None, None, batch_1()

        with engine.connect() as conn:
            results_after_batch_1 = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

        yield None, None, batch_2()
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
            None, None,
            (
                (my_table, (1, 2, 'a', 'b')),
                (my_table, (3, 4, 'c', 'd')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    oid_1 = _get_table_oid(engine, my_table)

    batches_2 = lambda _: (
        (
            None, None,
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

    oid_2 = _get_table_oid(engine, my_table)

    assert results == [
        (1, 2, 'a', 'b'),
        (3, 4, 'e', 'f'),
        (3, 6, 'g', 'h'),
    ]

    assert oid_1 == oid_2
    assert len(metadata.tables) == 1


def test_upsert_off():
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
            None, None,
            (
                (my_table, (1, 2, 'a', 'b')),
                (my_table, (3, 4, 'c', 'd')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    oid_1 = _get_table_oid(engine, my_table)

    batches_2 = lambda _: (
        (
            None, None,
            (
                (my_table, (3, 4, 'e', 'f')),
                (my_table, (3, 6, 'g', 'h')),
            ),
        ),
    )
    with \
            pytest.raises(Exception), \
            engine.connect() as conn:
        ingest(conn, metadata, batches_2, upsert=Upsert.OFF)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id\"\'_2🍰')).fetchall()

    assert results == [
        (1, 2, 'a', 'b'),
        (3, 4, 'c', 'd'),
    ]

    assert len(metadata.tables) == 1


def test_upsert_with_duplicates_in_batch():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    my_table = sa.Table(
        "mY_t\"\'able_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        sa.Column("id_2", sa.INTEGER, primary_key=True),
        sa.Column("Value_1", sa.VARCHAR),
        sa.Column("value_2", sa.VARCHAR),
        schema="my_Schema",
    )
    batches_1 = lambda _: (
        (
            None, None,
            (
                (my_table, (1, 2, 'a', 'b')),
                (my_table, (3, 4, 'c', 'd')),
                (my_table, (1, 2, 'a', 'c')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    oid_1 = _get_table_oid(engine, my_table)

    batches_2 = lambda _: (
        (
            None, None,
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
        results = conn.execute(sa.select(my_table).order_by('id_1', 'id_2')).fetchall()

    oid_2 = _get_table_oid(engine, my_table)

    assert results == [
        (1, 2, 'a', 'c'),
        (3, 4, 'e', 'f'),
        (3, 6, 'g', 'h'),
    ]

    assert oid_1 == oid_2
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
        yield (high_watermark or 0) + 1,  None,()
        yield (high_watermark or 0) + 2,  None,()
        yield (high_watermark or 0) + 3,  None,()

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
        yield (high_watermark or 0) + 1,  None, ()
        yield (high_watermark or 0) + 2,  None, ()
        yield (high_watermark or 0) + 3,  None, ()

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
        yield (high_watermark or 0) + 1, None, ()
        if (high_watermark or 0) > 4:
            raise Exception()
        yield (high_watermark or 0) + 3, None, ()

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
        yield (high_watermark or 0) + 1,  None, ()
        yield (high_watermark or 0) + 2,  None, ()
        yield (high_watermark or 0) + 3,  None, ()

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
            None, None,
            (
                (my_table_1, (1, 'a')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    oid_1 = _get_table_oid(engine, my_table_1)

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
            batch_2_result = conn.execute(sa.select(my_table_1).order_by('id')).fetchall()

        yield None, None, (
            (my_table_2, (2, 'b', 'c')),
        )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    oid_2 = _get_table_oid(engine, my_table_2)

    assert batch_2_result == [
        (1, 'a'),
    ]
    assert results == [
        (1, 'a', None),
        (2, 'b', 'c'),
    ]

    assert oid_1 != oid_2
    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_table_with_multiple_similar_indexes():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_1",
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("a", sa.VARCHAR),
        sa.Column("b", sa.VARCHAR),
        sa.Index(None, "a", "b"),
        sa.Index(None, "a"),
        schema="my_schema",
    )

    with engine.connect() as conn:
        ingest(conn, metadata_1, _no_batches)

    # We're mostly just checking is that no exception is raised
    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id')).fetchall()

    assert results == []


def test_migrate_add_index():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches)

    oid_1 = _get_table_oid(engine, my_table_1)

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1",sa.VARCHAR),
        sa.Index(None, "value_1"),
        sa.Index(None, "value_1", "id"),
        schema="my_schema",
    )

    with engine.connect() as conn:
        ingest(conn, metadata_2, batches)

    oid_2 = _get_table_oid(engine, my_table_2)

    with engine.connect() as conn:
        live_table = sa.Table(my_table_2.name, sa.MetaData(), schema=my_table_2.schema, autoload_with=conn)
        # Reflection can return a different server_default even if client code didn't set it
        for column in live_table.columns.values():
            column.server_default = None

    # Indexes must be the same up to their name, which is ignored
    @contextmanager
    def ignored_name(indexes):
        names = [index.name for index in indexes]
        for index in indexes:
            index.name = '__IGNORE__'
        try:
            yield
        finally:
            for name, index in zip(names, indexes):
                index.name = name
    with \
            ignored_name(live_table.indexes), \
            ignored_name( my_table_2.indexes):
        indexes_live_table = set(tuple(repr(index) for index in live_table.indexes))
        indexes_my_table_2 = set(tuple(repr(index) for index in my_table_2.indexes))

    assert indexes_live_table == indexes_my_table_2
    assert oid_1 != oid_2
    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_migrate_add_gin_index():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.ARRAY(sa.VARCHAR)),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (),
        ),
    )

    with engine.connect() as conn:
        ingest(conn, metadata_1, batches)

    oid_1 = _get_table_oid(engine, my_table_1)

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.ARRAY(sa.VARCHAR)),
        sa.Index(None, "value_1", postgresql_using='gin'),
        schema="my_schema",
    )
    assert next(iter(my_table_2.indexes)).dialect_kwargs['postgresql_using'] == 'gin'

    with engine.connect() as conn:
        ingest(conn, metadata_2, batches)

    oid_2 = _get_table_oid(engine, my_table_2)

    with engine.connect() as conn:
        live_table = sa.Table(my_table_2.name, sa.MetaData(), schema=my_table_2.schema, autoload_with=conn)

    assert next(iter(live_table.indexes)).dialect_kwargs['postgresql_using'] == 'gin'

    assert oid_1 != oid_2

    metadata_3 = sa.MetaData()
    my_table_3 = sa.Table(
        my_table_1.name,
        metadata_3,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.ARRAY(sa.VARCHAR)),
        sa.Index(None, "value_1", postgresql_using='gin', postgresql_include=[]),
        schema="my_schema",
    )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches)

    oid_3 = _get_table_oid(engine, my_table_2)

    with engine.connect() as conn:
        live_table = sa.Table(my_table_2.name, sa.MetaData(), schema=my_table_2.schema, autoload_with=conn)

    assert next(iter(live_table.indexes)).dialect_kwargs['postgresql_using'] == 'gin'

    assert oid_2 == oid_3


def test_migrate_remove_index():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata_1 = sa.MetaData()
    my_table_1 = sa.Table(
        "my_table_" + uuid.uuid4().hex,
        metadata_1,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR, index=True),
        schema="my_schema",
    )
    batches = lambda _: (
        (
            None, None,
            (),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches)

    oid_1 = _get_table_oid(engine, my_table_1)
    with engine.connect() as conn:
        live_table = sa.Table(my_table_1.name, sa.MetaData(), schema=my_table_1.schema, autoload_with=conn)
    assert len(live_table.indexes) == 1

    metadata_2 = sa.MetaData()
    my_table_2 = sa.Table(
        my_table_1.name,
        metadata_2,
        sa.Column("id", sa.INTEGER, primary_key=True),
        sa.Column("value_1", sa.VARCHAR),
        schema="my_schema",
    )

    with engine.connect() as conn:
        ingest(conn, metadata_2, batches)

    oid_2 = _get_table_oid(engine, my_table_2)

    with engine.connect() as conn:
        live_table = sa.Table(my_table_2.name, sa.MetaData(), schema=my_table_2.schema, autoload_with=conn)

    assert len(live_table.indexes) == 0

    assert oid_1 != oid_2
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
            None, None,
            (
                (my_table_1, (1, 'a', 'b')),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    oid_1 = _get_table_oid(engine, my_table_1)

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
            batch_2_result = conn.execute(sa.select(my_table_1).order_by('id')).fetchall()

        yield None, None, (
            (my_table_2, (2, 'a', 'c', 'b')),
        )
    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    oid_2 = _get_table_oid(engine, my_table_2)

    assert batch_2_result == [
        (1, 'a', 'b'),
    ]
    assert results == [
        (1, 'a', None, 'b'),
        (2, 'a', 'c', 'b'),
    ]

    assert oid_1 != oid_2
    assert len(metadata_1.tables) == 1
    assert len(metadata_2.tables) == 1


def test_migrate_add_column_not_at_end_batch_fails_high_watermark_preserved():
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

    high_watermarks = []
    def batches_1(high_watermark):
        high_watermarks.append(high_watermark)
        yield 1, None, ()

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
    def batches_2(high_watermark):
        high_watermarks.append(high_watermark)
        yield from ()
        raise Exception()

    with pytest.raises(Exception):
        with engine.connect() as conn:
            ingest(conn, metadata_2, batches_2)

    def batches_3(high_watermark):
        high_watermarks.append(high_watermark)
        yield from ()

    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_3)

    assert high_watermarks == [None, 1, 1]


def test_migrate_add_column_not_at_end_permissions_preserved():
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

    def batches_1(high_watermark):
        yield 1, None, ((my_table_1, (1, 'a', 'b')),)

    with engine.connect() as conn:
        ingest(conn, metadata_1, batches_1)

    user_id = uuid.uuid4().hex[:16]
    with engine.connect() as conn:
        conn.execute(sa.text(sql.SQL('''
             CREATE USER {user_id} WITH PASSWORD 'password';
        ''').format(user_id=sql.Identifier(user_id)).as_string(conn.connection.driver_connection)))
        conn.execute(sa.text(sql.SQL('''
             GRANT CONNECT ON DATABASE postgres TO {user_id};
        ''').format(user_id=sql.Identifier(user_id)).as_string(conn.connection.driver_connection)))
        conn.execute(sa.text(sql.SQL('''
             GRANT USAGE ON SCHEMA my_schema TO {user_id};
        ''').format(user_id=sql.Identifier(user_id)).as_string(conn.connection.driver_connection)))
        conn.commit()
        conn.execute(sa.text(sql.SQL('''
             GRANT SELECT ON my_schema.{table} TO {user_id};
        ''').format(table=sql.Identifier(my_table_1.name), user_id=sql.Identifier(user_id)).as_string(conn.connection.driver_connection)))
        conn.commit()

    user_engine = sa.create_engine(f'{engine_type}://{user_id}:password@127.0.0.1:5432/postgres', **engine_future)
    with user_engine.connect() as conn:
        results = conn.execute(sa.select(my_table_1).order_by('id')).fetchall()

    assert results == [(1, 'a', 'b')]

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

    def batches_2(high_watermark):
        yield (None, None, ())

    with engine.connect() as conn:
        ingest(conn, metadata_2, batches_2)

    with user_engine.connect() as conn:
        results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

    assert results == [(1, 'a', None, 'b')]


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
            None, None,
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
        results = conn.execute(sa.select(my_table_1).order_by('id')).fetchall()

    assert results == [
        (1, 'a', 'b'),
    ]

    with pytest.raises(Exception):
        with engine.connect() as conn:
            results = conn.execute(sa.select(my_table_2).order_by('id')).fetchall()

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
            None, None,
            (
                (my_table, (1,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    batches_2 = lambda _: (
        (
            None, None,
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
            None, None,
            (
                (my_table, (1,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_1)

    batches_2 = lambda _: (
        (
            None, None,
            (
                (my_table, (2,)),
            ),
        ),
    )
    with engine.connect() as conn:
        ingest(conn, metadata, batches_2, delete=Delete.BEFORE_FIRST_BATCH)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (2,),
    ]

    with engine.connect() as conn:
        ingest(conn, metadata, _no_batches, delete=Delete.BEFORE_FIRST_BATCH)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (2,),
    ]

    assert len(metadata.tables) == 1


def test_on_before_batch_visible():
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
            None, 'Batch metadata 1',
            (
                (my_table, (1,)),
            ),
        ),
        (
            None, 'Batch metadata 2',
            (
                (my_table, (2,)),
            ),
        ),
    )
    results_in_batch_connection = []
    results_out_of_batch_connections = []
    batch_metadatas = []
    def on_before_visible(conn, ingest_table, batch_metadata):
        results_in_batch_connection.append(conn.execute(sa.select(ingest_table).order_by('integer')).fetchall())
        batch_metadatas.append(batch_metadata)
        with engine.connect() as conn_out:
            try:
                results_out_of_batch_connections.append(conn_out.execute(sa.select(my_table).order_by('integer')).fetchall())
            except:
                results_out_of_batch_connections.append([])

    with engine.connect() as conn:
        ingest(conn, metadata, batches, on_before_visible=on_before_visible)

    with engine.connect() as conn:
        results = conn.execute(sa.select(my_table).order_by('integer')).fetchall()

    assert results == [
        (1,),
        (2,),
    ]
    assert results_in_batch_connection == [[(1,)], [(1,), (2,)]]
    assert results_out_of_batch_connections == [[], [(1,)]]
    assert batch_metadatas == ['Batch metadata 1', 'Batch metadata 2']


def test_high_watermark_with_earliest():
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
        yield (high_watermark or 0) + 1,  None,()
        yield (high_watermark or 0) + 2,  None,()
        yield (high_watermark or 0) + 3,  None,()

    with engine.connect() as conn:
        ingest(conn, metadata, batches, high_watermark="__EARLIEST__")

    assert high_watermarks == [None]

    with engine.connect() as conn:
        ingest(conn, metadata, batches, high_watermark="__EARLIEST__")

    assert high_watermarks == [None, None]


    with engine.connect() as conn:
        ingest(conn, metadata, batches, high_watermark="__EARLIEST__")

    assert high_watermarks == [None, None, None]


def test_high_watermark_callable():
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
    today = '2011-01-01'

    def batches(high_watermark):
        high_watermarks.append(high_watermark)
        yield lambda: today, None, ()

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None]

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    assert high_watermarks == [None, today]


def test_multiple_tables():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/', **engine_future)

    metadata = sa.MetaData()
    table_1 = sa.Table(
        "table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        schema="my_schema",
    )
    table_2 = sa.Table(
        "table_" + uuid.uuid4().hex,
        metadata,
        sa.Column("id_1", sa.INTEGER, primary_key=True),
        schema="my_schema",
    )

    def batch():
        for i in range(0, 20000):
            yield table_1, (i,)
            yield table_2, (i,)

    def batches(high_watermark):
        yield None, None, batch()

    with engine.connect() as conn:
        ingest(conn, metadata, batches)

    with engine.connect() as conn:
        results_a = conn.execute(sa.select(table_1).order_by('id_1')).fetchall()
        results_b = conn.execute(sa.select(table_2).order_by('id_1')).fetchall()

    assert results_a == [(i,) for i in range(0, 20000)]
    assert results_b == [(i,) for i in range(0, 20000)]


def test_to_file_object_function():
    iterable=[b'1',b'2',b'3']
    obj = to_file_like_obj(iterable, bytes)
    obj_read = obj.read()
    assert obj_read == b'123'

def test_str_to_file_like_obj():
    iterable=['1','2','3']
    obj = to_file_like_obj(iterable, str)
    obj_read = obj.read()
    assert obj_read == '123'

def test_read_less_bytes_in_to_file_object_function():
    iterable=[b'1',b'2',b'3']
    obj = to_file_like_obj(iterable, bytes)
    obj_read = obj.read(size=1)
    assert obj_read == b'1'

def test_read_more_bytes_than_available_in_to_file_object_function():
    iterable=[b'1',b'2',b'3']
    obj = to_file_like_obj(iterable, bytes)
    obj_read = obj.read(size=10)
    assert obj_read == b'123'

def test_reading_file_obj_twice():
    iterable=[b'1',b'2',b'3']
    obj = to_file_like_obj(iterable, bytes)
    obj_read = obj.read()
    assert obj_read == b'123'
    obj_read_twice = obj.read()
    assert obj_read_twice == b''

def test_negative_or_none_sizes():
    iterable=[b'1',b'2',b'3']
    obj = to_file_like_obj(iterable, bytes)
    obj_read = obj.read(-2)
    print(obj_read)
    assert obj_read == b'123'
    obj_2 = to_file_like_obj(iterable, bytes)
    obj_read_2 = obj_2.read(None)
    print(obj_read_2)
    assert obj_read_2 == b'123'

def test_streaming_behaviour_of_to_file_object():
    total_read = 0
    def with_count(iter_bytes):
        nonlocal total_read
        for chunk in iter_bytes:
            total_read += len(chunk)
            yield chunk
    f = to_file_like_obj(with_count((b'a', b'b')), bytes)
    f.read(1)
    assert total_read == 1

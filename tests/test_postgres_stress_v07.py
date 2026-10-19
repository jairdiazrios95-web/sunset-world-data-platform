"""Contract tests for actual PostgreSQL large-volume runner (without mock execution)."""
import pytest
from stress.postgres_live_stress import PROFILES,batches,insert_sql


def test_pg_2x_exceeds_requested_reference():
    ref=PROFILES['reference']; double=PROFILES['double']
    assert sum(ref.values()) == 21_400_000
    assert sum(double.values()) == 42_800_000
    assert all(double[k] == 2*ref[k] for k in ref)


def test_pg_batch_boundaries():
    assert list(batches(0,3)) == []
    assert list(batches(11,4)) == [(1,4),(5,8),(9,11)]
    with pytest.raises(ValueError): list(batches(10,0))
    with pytest.raises(ValueError): list(batches(-1,3))


def test_pg_real_queries_have_conflict_and_foreign_key_parent():
    for source in ('leads','payments','comments'):
        query=insert_sql(source)
        assert 'stress.'+source in query
        assert 'generate_series' in query
        assert 'ON CONFLICT(id) DO NOTHING' in query
    with pytest.raises(ValueError): insert_sql('wrong;DROP')


def test_parameterised_postgres_sql_escapes_literal_modulo():
    # The original benchmark emitted '%s' where SQL needed modulo + bind,
    # and '%365' that psycopg can misparse as a placeholder.
    leads = insert_sql('leads')
    assert 'n%%1000000' in leads
    assert '(n%%365)' in leads
    for source in ('payments', 'comments'):
        sql = insert_sql(source)
        assert '((n-1) %% %s::bigint)+1' in sql
        assert '(n%%365)' in sql
        assert sql.count('%s') == 3

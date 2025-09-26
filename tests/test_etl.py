from etl_job import run_job # type: ignore

def test_avg_age():
    result = run_job()
    assert round(result, 1) == 31.7


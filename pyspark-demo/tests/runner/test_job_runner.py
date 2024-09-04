from psk.runner.job_runner import JobRunner


def test_job_runner():
    runner = JobRunner("dummy")

    assert runner.job_name == "dummy"
    assert runner.job_main is not None

    runner.run()

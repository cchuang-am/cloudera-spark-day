import importlib


class JobRunner:
    def __init__(self, job_name):
        self.job_name = job_name
        self.job_main = importlib.import_module(f"dsk.job.{job_name}.main")

    def run(self):
        self.job_main.run()

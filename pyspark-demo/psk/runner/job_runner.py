import importlib

from psk.service import spark_service


class JobRunner:
    def __init__(self, job_name):
        self.job_name = job_name
        self.job_main = importlib.import_module(f"psk.job.{job_name}.main")
        self.spark = spark_service.create_spark()

    def run(self):
        self.job_main.run(self.spark)

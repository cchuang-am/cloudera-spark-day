import sys

from dsk.runner.job_runner import JobRunner


def main(args):
    job_name = args[1]
    runner = JobRunner(job_name)
    runner.run()


if __name__ == '__main__':
    main(sys.argv)

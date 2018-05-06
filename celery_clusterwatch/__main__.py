from celery.bin.base import Command
import os
from .monitor import main as monitor


class ClusterWatchCommand(Command):
    """Monitors the execution of Celery tasks
    and uploads the results of succeeded and
    failed tasks to AWS CloudWatch."""

    def add_arguments(self, parser):
        """Allows us to expose additional command
        line options.
        Arguments:
            parser:
                The :see:argparse parser to add
                the command line options to.
        """

        parser.add_argument(
            '--frequency', default=1,
            help='Frequency of the camera'
        )

    def run(self, broker=None, frequency=60, **kwargs):
        """Invoked when the user runs `celery cloudwatch`."""        
        if broker is None:
            broker = os.getenv('CELERY_BROKER', None)
        monitor(broker, int(frequency))


# if __name__ == '__main__':
#     monitor()
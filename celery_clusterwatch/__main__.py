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

        parser.add_argument(
            '--region',
            help='AWS CloudWatch Region'
        )

        parser.add_argument(
            '--access_key',
            help='AWS Access Key'
        )

        parser.add_argument(
            '--secret_key',
            help='AWS Secret Key'
        )

    def run(self, broker=None, frequency=60, region=None, access_key=None, secret_key=None, **kwargs):
        """Invoked when the user runs `celery cloudwatch`."""        
        if broker is None:
            broker = os.getenv('CELERY_BROKER', None)

        if region is None:
            raise ValueError('--region')
        
        if access_key is None:
            raise ValueError('--access_key')

        if secret_key is None:
            raise ValueError('--secret_key')

        monitor(broker, int(frequency), region, access_key, secret_key)


# if __name__ == '__main__':
#     monitor()
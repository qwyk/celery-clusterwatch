from celery import Celery
from celery.events.snapshot import Polaroid
from pprint import pformat

class Camera(Polaroid):
    clean_after = True
    def on_shutter(self, state):
        try:
            for worker in state.alive_workers():
                print(worker.processed, worker.status_string, worker.id, worker.loadavg, worker.hostname)
        except:
            raise
        # print('Workers: {0}'.format(pformat(state.workers, indent=4)))
        # print('Tasks: {0}'.format(pformat(state.tasks, indent=4)))
        # print('Total: {0.event_count} events, {0.task_count} tasks'.format(state))

def main(broker_url, frequency):
    app = Celery(broker=broker_url)
    state = app.events.State()
    for worker in state.alive_workers():
        print(worker.loadavg)
    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={'*': state.event})
        with Camera(state, freq=frequency):
            recv.capture(limit=None, timeout=None)
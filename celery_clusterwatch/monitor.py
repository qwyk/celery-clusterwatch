from celery import Celery
from celery.events.snapshot import Polaroid
from pprint import pformat
import json
import sys
import pandas as pd
import numpy as np
import boto3
from datetime import datetime



class Camera(Polaroid):

    def __init__(self, state, client, freq=1.0, maxrate=None, cleanup_freq=3600.0, timer=None, app=None):
        self.client = client
        super(Camera, self).__init__(state, freq=freq, maxrate=maxrate, cleanup_freq=cleanup_freq, timer=timer, app=app)

    clear_after = True
    tasks = []
    
    def on_shutter(self, state):        
        try:
            tasks = []
            for task in state.tasks_by_time():
                t = task[1].as_dict()
                t['worker'] = str(t['worker'].id)
                tasks.append(t)
            if len(tasks) > 0:
                p = pd.DataFrame(tasks)
                p['worker'] = [w.split('@')[0] for w in p['worker']]
                # p.set_index(['worker', 'name', 'state']).stack().groupby(level=0).describe().unstack()

                def agg(x):
                    
                    names = {
                        'mean': x['runtime'].mean(),
                        'min':  x['runtime'].min(),
                        'max': x['runtime'].max(),
                        'count':  x['uuid'].count()
                        }
                    return pd.Series(names, index=['mean', 'min', 'max', 'count'])

                g = p.groupby(['worker', 'name', 'state']).apply(agg).reset_index()
                print(g)
                metric_data = []

                for i in g.values.tolist():
                    if i[2] == 'SUCCESS':
                        metric_data.append({
                            'MetricName': 'MeanTaskTime',
                            'Dimensions': [
                                {
                                    'Name': 'Region',
                                    'Value': i[0]
                                }, 
                                {
                                    'Name': 'TaskName',
                                    'Value': i[1]
                                }
                            ],
                            'Timestamp': datetime.utcnow(),
                            'Value': i[3],
                            'Unit': 'Seconds',            
                            'StorageResolution': 60
                        })

                    metric_data.append({
                            'MetricName': 'TaskCount',
                            'Dimensions': [
                                {
                                    'Name': 'Region',
                                    'Value': i[0]
                                }, 
                                {
                                    'Name': 'TaskName',
                                    'Value': i[1]
                                },
                                {
                                    'Name': "State",
                                    "Value": i[2]
                                }
                            ],
                            'Timestamp': datetime.utcnow(),
                            'Value': i[6],
                            'Unit': 'Count',            
                            'StorageResolution': 60
                        })
                    
                self.client.put_metric_data(
                                Namespace='QWYK/QSS',
                                MetricData=metric_data)
        except:
            with open('test.txt', 'w') as file:
                file.write(str(sys.exc_info()[1]))

def main(broker_url, frequency, aws_region, aws_access_key, aws_secret):
    
    app = Celery(broker=broker_url)
    state = app.events.State()
    client = boto3.client('cloudwatch', aws_region, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret)
    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={'*': state.event})
        with Camera(state, client, freq=frequency):
            recv.capture(limit=None, timeout=None)
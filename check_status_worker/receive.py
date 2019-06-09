import time
import logging
import functools

logging.basicConfig(level = logging.INFO)

from google.cloud import pubsub_v1

# Modify the project ID to match your own
# This project ID will NOT work for you
project_id = "isb-cgc-01-0006"
topic_name = "my-topic"

subscription_name="my-sub"

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path(
    project_id, subscription_name)

def cache(fn=None,time_to_live=3600*24): # one DAY default (or whatever)
    if not fn: return functools.partial(cache,time_to_live=time_to_live)
    my_cache = {}
    def _inner_fn(*args,**kwargs):
        kws = sorted(kwargs.items()) # in python3.6+ you dont need sorted
        key = tuple(args)+tuple(kws) 
        if key not in my_cache or time.time() > my_cache[key]['expires']:
               my_cache[key] = {"value":fn(*args,**kwargs),"expires":time.time()+ time_to_live}
        return my_cache[key]
    return _inner_fn

DONE=1
NOTDONE=2
ERROR=3
RETRY=4

@cache(time_to_live=60*3)
def check_job_status(job_id):
    logging.info('checking status')
    return(DONE)


def callback(message):
    import subprocess
    print('Received message: {}'.format(message))
    res = subprocess.run(["ls", "-l", "/dev/null"], capture_output=True)
    import time
    time.sleep(20)
    print(check_job_status(1))
        
    if(res.returncode != 0):
        message.nack()
    logging.info(res.stdout)
    logging.info(res.stderr)
    message.ack()

fc = pubsub_v1.types.FlowControl(max_messages=1)
subscriber.subscribe(subscription_path, callback=callback, flow_control = fc)

# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(60)

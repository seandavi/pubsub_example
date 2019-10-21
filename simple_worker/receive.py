import time
import logging

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


def callback(message):
    """Will be run for each message"""
    import subprocess
    print('Received message: {}'.format(message))
    # Doing a silly ls here, but can be any command, basically.
    res = subprocess.run(["ls", "-l", "/dev/null"], capture_output=True)
    import time
    time.sleep(20)
    # If returncode is not 0, let pubsub know that the
    # job is not acknowledged. (put it back in queue)
    if(res.returncode != 0):
        message.nack()
    logging.info(res.stdout)
    logging.info(res.stderr)
    # Acknowledge completed message (remove from queue)
    message.ack()

fc = pubsub_v1.types.FlowControl(max_messages=1)
subscriber.subscribe(subscription_path, callback=callback, flow_control = fc)

# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(60)

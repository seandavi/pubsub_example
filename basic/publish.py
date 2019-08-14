from google.cloud import pubsub
import logging

# Modify the project ID to match your own
# This project ID will NOT work for you
project_id = "isb-cgc-01-0006"
topic_name = "my-topic" 

publisher = pubsub.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_name}`
topic_path = publisher.topic_path(project_id, topic_name)

def send_message(msg):
    data = msg.encode('utf-8')
    attributes = {'format':'json',
                  'published':'now'}
    future = publisher.publish(topic_path, data = data, **attributes)
    logging.info('Published {} of message ID {}.'.format(data, future.result()))

if __name__ == '__main__':
    import sys
    send_message(sys.argv[1])

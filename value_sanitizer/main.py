import base64
import time
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd
import io
import csv
from io import BytesIO
import json

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("nyc-ticketing-system", "nyc-function-1")
topic_path2 = publisher.topic_path('nyc-ticketing-system', 'nyc-data')
def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    #print(type(pubsub_message))
    mess = json.loads(pubsub_message)
    #print(type(mess))
    important = ['Street Name', 'Plate ID', 'Violation Code', 'Issue Date']
    flag = True
    ans = {}
    for val in important:
        if mess.get(val, None) is None:
             flag = False
        else:
            ans[val] = mess[val]

    if flag:
        data = str(ans)
        data = data.encode("utf-8")
        publisher.publish(topic_path, data=data).result()

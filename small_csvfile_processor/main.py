import time
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd
import io
import csv
from io import BytesIO

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('nyc-ticketing-system', 'nyc-bucket-1')

def check_columns(data, context):
    try:
        object_name = data['name']
        bucket_name = data['bucket']

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        data = blob.download_as_string()

        df = pd.read_csv(BytesIO(data), names = ['Summons Number', 'Plate ID', 'Registration State', 'Plate Type', 'Issue Date', 'Violation Code', 'Vehicle Body Type', 'Vehicle Make', 'Issuing Agency', 'Street Code1', 'Street Code2', 'Street Code3', 'Vehicle Expiration Date', 'Violation Location', 'Violation Precinct', 'Issuer Precinct', 'Issuer Code', 'Issuer Command', 'Issuer Squad', 'Violation Time', 'Time First Observed', 'Violation County', 'Violation In Front Of Or Opposite', 'House Number', 'Street Name', 'Intersecting Street', 'Date First Observed', 'Law Section', 'Sub Division', 'Violation Legal Code', 'Days Parking In Effect    ', 'From Hours In Effect', 'To Hours In Effect', 'Vehicle Color', 'Unregistered Vehicle?', 'Vehicle Year', 'Meter Number', 'Feet From Curb', 'Violation Post Code', 'Violation Description', 'No Standing or Stopping Violation', 'Hydrant Violation', 'Double Parking Violation', 'Latitude', 'Longitude', 'Community Board', 'Community Council ', 'Census Tract', 'BIN', 'BBL', 'NTA'])
        #fileName = "gs://" + bucket_name + "/" + object_name
        #df = pd.read_csv(filename)
        a = list(df)
        print(a)
        print(df.shape)
        df['json'] = df.apply(lambda x: x.to_json(), axis=1)
        print(df.shape)

        for ind in df.index:
            if ind%500 == 0:
                print(ind)
            data = df['json'][ind]
            #data = df[ind].apply(lambda x: x.to_json(), axis=1)
            data = data.encode("utf-8")
            #print(type(data))
            #print(topic_path)
            #future = publisher.publish(topic_path, data=data)
            publisher.publish(topic_path, data=data).result()
            #time.sleep(10)
            #print(future.result())
        print("Completed!")
    except:
        print("Something went wrong!")

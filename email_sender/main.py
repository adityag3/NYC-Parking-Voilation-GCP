import base64
import time
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd
import io
import csv
from io import BytesIO
import json
import requests
from mailjet_rest import Client
import os

def hello_pubsub(event, context):
	"""Triggered from a message on a Cloud Pub/Sub topic.
	Args:
		 event (dict): Event payload.
		 context (google.cloud.functions.Context): Metadata for the event.
	"""
	pubsub_message = base64.b64decode(event['data']).decode('utf-8')
	pubsub_message = pubsub_message.replace("'", "\"")
	print(pubsub_message)
	print(type(pubsub_message))
	mess = json.loads(pubsub_message)
	
	
	r = requests.get(url = "https://nyc-ticketing-system.ue.r.appspot.com/",params={"plateid": str(mess['Plate ID'])})
	if r.status_code == 200:
		mail_id = r.content.decode("utf-8") 
		api_key = '43d2aa8f3b75df68ada529dfdaa9feb6'
		api_secret = '625e23a252d4a299006e0883201aa530'
		mailjet = Client(auth=(api_key, api_secret), version='v3.1')
		data = {
		  'Messages': [
			{
			  "From": {
				"Email": "sayini7974@stevefotos.com",
				"Name": "Aditya"
			  },
			  "To": [
				{
				"Email": mail_id,
				"Name": "Sir/Ma'am"
				}
			  ],
			"Subject": "Parking Voilation",
			"TextPart": "You have been issued a parking ticket",
			"HTMLPart": "<h3>Sir/Ma'am, Please visit this <a href='https://www.mailjet.com/'>website</a> for more information</h3>",
			"CustomID": "AppGettingStartedTest"
			}
			]
		}
		result = mailjet.send.create(data=data)
		print(result.status_code)
		print(result.json())
	else:
		print("Something went wrong!")

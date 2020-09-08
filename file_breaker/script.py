import os
from google.cloud import storage
from io import StringIO

# client
storage_client = storage.Client.from_service_account_json("/home/aditya/Downloads/polar-scene-286016-a839bb9d7520.json")

# Google Cloud Storage
bucketName = "adityag1"
bucketName2 = "citation_raw_data"

bucket = storage_client.get_bucket(bucketName)
bucket2 = storage_client.get_bucket(bucketName2)

fn = list(bucket.list_blobs(prefix=''))

for file in fn:
	if '.' in str(file):
		s=str(file.download_as_string(),"utf-8")
		data = StringIO(s) 
		df=pd.read_csv(data)

		filename = 1
		for i in range(len(csvfile)//2):
			if i % 5000 == 0:
				name = str(filename) + '.csv'
				open(name, 'w+').writelines(csvfile[i:i+5000])
				filename += 1

				blob = bucket2.blob(name)
				blob.upload_from_filename(name)

				os.remove(name)

		file.delete()

"""UPLOAD CSV FILE TO S3 USING RESOURCE AND STRINGIO"""
bucket = 'test_bucket'
key = 'key_folder/key_file.csv'
resource = boto3.resource('s3')
csv_buffer = StringIO()
df.to_csv(csv_buffer)
resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

"""DOWNLOAD CSV FILE FROM S3 USING RESOURCE"""
obj = resource.Object(bucket, aws_key).get()
data = pd.read_csv(obj['Body'])

"""DOWNLOAD CSV FILE FROM S3 USING CLIENT"""
client = boto3.client('s3')
obj = client.get_object(Bucket=bucket, Key=aws_key)j
data = pd.read_csv(obj['Body'])

"""DOWNLOAD GROUP OF FILES FROM S3 USING RESOURCE AND BYTESIO"""
resource = boto3.resource('s3')
bucket = resource.Bucket('test_buket')
files = list(bucket.objects.filter(Prefix=test_key_folder/sub_folder/'))
model_names = {}

for x in range(len(files)):
    obj = BytesIO(files[x].get()['Body'].read())
#     EXAMPLE OF READING CSV THAT IS ZIPPED.  NEED TO USE IO WRAPPER    
#     data = pd.read_csv(gzip.open(BytesIO(obj['Body'].read())))
    model_names[str(files[x]).split('/')[-1].replace("')", "")] = obj

""" PICKLE AND UPLOAD FILES TO S3 USING RESOURCE"""
pickle_obj = pickle.dumps(model_object, protocol=0)
s3 = boto3.resource('s3')
s3.Object(self.bucket, aws_key).put(Body=pickle_obj)

"""SAVE OBJECT TO S3 THAT IS NOT CSV"""

bucket = 'bucket'
aws_key = 'test_folder/subfolder/blah'
s3 = boto3.resource('s3')
buffer = StringIO()
buffer.write(str(soup))
s3.Object(bucket, aws_key).put(Body=buffer.getvalue())
buffer.close()

"""READ OBJECT FROM S3 THAT IS NOT CSV - Decode if necessary"""

bucket = 'bucket'
aws_key = 'folder?subfolder/file'
s3 = boto3.resource('s3')
test = resource.Object(bucket, key).get()
test_data = test['Body'].read().decode('latin1')

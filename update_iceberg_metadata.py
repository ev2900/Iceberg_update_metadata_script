import boto3
import io
import fastavro
import json

def update_avro(bucket_name, object_key, old_bucket_name_or_path, new_bucket_name_or_path):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Store the file as byte object
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # The file content is in response['Body']. Read it as binary.
    binary_content = response['Body'].read()

    # Use io.BytesIO to create a file-like object for fastavro to read from
    binary_stream = io.BytesIO(binary_content)

    # Open the Avro file using fastavro
    reader = fastavro.reader(binary_stream)

    # Read all records into a list
    records = list(reader)

    # Iterate over records in the Avro file
    for record in records:
        for key, value in record.items():
            # If it is a nested dictionary, check its items
            if isinstance(value, dict):  
                for k, v in value.items():
                    if type(v) is str:
                        if 's3://' in v: 
                            value[k] = v.replace(old_bucket_name_or_path, new_bucket_name_or_path)        
    
            # If it's a list, check each element
            elif isinstance(value, list):
                for i in range(len(value)):
                    if isinstance(value[i], dict):
                        for k, v in value[i].items():
                            if type(v) is str:
                                if 's3://' in v: 
                                    value[k] = v.replace(old_bucket_name_or_path, new_bucket_name_or_path)
            else:
                if type(value) is str:
                    if 's3://' in value:
                        record[key] = value.replace(old_bucket_name_or_path, new_bucket_name_or_path)

    # Create a file-like object in memory to hold the Avro data
    output = io.BytesIO()

    # Read the schema from the input AVRO file
    schema = reader.schema
    
    # Write the data to the in-memory file using fastavro
    fastavro.writer(output, schema, records)
    
    # Move the cursor to the start of the file-like object to read from it
    output.seek(0)
    
    # Upload the Avro file to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=output.getvalue())

def update_json(bucket_name, object_key, old_bucket_name_or_path, new_bucket_name_or_path):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Read JSON file from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    json_content = response['Body'].read().decode('utf-8')
    
    # Load the JSON data into a Python object
    data = json.loads(json_content)
    
    # Traverse and modify the JSON data
    if isinstance(data, dict):
        # Iterate through dictionary
        for key in data:
            if isinstance(data[key], str):
                data[key] = data[key].replace(old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
            elif isinstance(data[key], list):
                for i, item in enumerate(data[key]):
                    if isinstance(item, str):
                        data[key][i] = item.replace(old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
                    elif isinstance(item, dict):
                        for sub_key in item:
                            if isinstance(item[sub_key], str):
                                item[sub_key] = item[sub_key].replace(old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
                                
    elif isinstance(data, list):
        # Iterate through list
        for i, item in enumerate(data):
            if isinstance(item, str):
                data[i] = item.replace(old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
            elif isinstance(item, dict):
                for key in item:
                    if isinstance(item[key], str):
                        item[key] = item[key].replace(old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
                                
    # Convert the modified data back to JSON
    modified_json_content = json.dumps(data, indent = 4)
    
    # Upload the modified JSON back to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=modified_json_content)
    
# Adjust the values of these variables before running the script
s3_bucket_name_w_metadata_to_update = 'register-iceberg-2ut1suuihxyq'
folder_path_to_metadata = 'iceberg/iceberg.db/sampledataicebergtable/metadata/'
old_s3_bucket_name_or_path = 'glue-iceberg-from-jars-s3bucket-2ut1suuihxyq'
new_s3_bucket_name_or_path = 'register-iceberg-2ut1suuihxyq'

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# List objects in the specified folder
response = s3.list_objects_v2(Bucket = s3_bucket_name_w_metadata_to_update, Prefix = folder_path_to_metadata)

# Check if the response contains 'Contents' (i.e., objects exist)
if 'Contents' in response:
    
    # Sort objects before processing them
    sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'])
    
    for obj in sorted_objects['Contents']:
        object_key = obj['Key']
        
        # Get the file extension
        file_extension = object_key.split('.')[-1]
        
        if file_extension == 'avro':
            update_avro(s3_bucket_name_w_metadata_to_update, object_key, old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
            
        elif file_extension == 'json':
            update_json(s3_bucket_name_w_metadata_to_update, object_key, old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
            
else:
    print("No objects found in the specified folder")

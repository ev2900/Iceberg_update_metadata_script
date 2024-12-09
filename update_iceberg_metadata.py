import boto3
import io
import fastavro
import json
import logging
import sys

def update_avro(bucket_name, object_key, old_bucket_name_or_path, new_bucket_name_or_path):
    logger.info(f"Updating AVRO file {object_key.split('/')[-1]}")

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
    logger.info(f"SUCCESSFULLY updated AVRO file {object_key.split('/')[-1]}")

def update_json(bucket_name, object_key, old_bucket_name_or_path, new_bucket_name_or_path):
    logger.info(f"Updating JSON file {object_key.split('/')[-1]}")
    
    # Read JSON file from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    json_content = response['Body'].read().decode('utf-8')
    
    # Load the JSON data into a Python object
    data = json.loads(json_content)
    
    # Function to search and replace a value in the JSON structure
    def replace_value(json_data, target_value, replacement_value):
        if isinstance(json_data, dict):  # If it's a dictionary, iterate over the keys and values
            for key, value in json_data.items():

                #
                # Fix required. Not working. Need to replace value == target_value with a search for partical
                #
                if value == target_value:
                    json_data[key] = replacement_value
                else:
                    replace_value(value, target_value, replacement_value)  # Recursively call for nested values
        elif isinstance(json_data, list):  # If it's a list, iterate over the items
            for index, item in enumerate(json_data):
                if item == target_value:
                    json_data[index] = replacement_value
                else:
                    replace_value(item, target_value, replacement_value)  # Recursively call for nested items

    # Replace the old_bucket_name_or_path with the new_bucket_name_or_path
    replace_value(data, old_bucket_name_or_path, new_bucket_name_or_path)
                                
    # Convert the modified data back to JSON
    modified_json_content = json.dumps(data, indent = 4)
    
    # Upload the modified JSON back to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=modified_json_content)
    logger.info(f"SUCCESSFULLY updated JSON file {object_key.split('/')[-1]}")

def find_most_recent_metadata_json(bucket_name, folder_path_to_metadata):
    # List objects in the specified folder
    response = s3.list_objects_v2(Bucket = bucket_name, Prefix = folder_path_to_metadata)
    
    # Create an empty dictionary that will have each metadata.json file name and its last updated time
    filename_lastupdate_dict = {}
    
    # Check if the response contains 'Contents' (i.e., objects exist)
    if 'Contents' in response:
        for obj in response['Contents']:
            object_key = obj['Key']
            
            # Get the file extension
            file_extension = object_key.split('.')[-1]
            
            if file_extension == 'json':
                    # Read JSON file from S3
                    response = s3.get_object(Bucket=bucket_name, Key=object_key)
                    json_content = response['Body'].read().decode('utf-8')
        
                    # Load the JSON data into a Python object
                    data = json.loads(json_content)
                    
                    filename_lastupdate_dict[object_key] = data['last-updated-ms']
                    
    # Find the most recent time (max value)
    most_recent_time = max(filename_lastupdate_dict.values())

    # Find the key corresponding to the most recent time
    most_recent_event = [key for key, value in filename_lastupdate_dict.items() if value == most_recent_time][0]
    
    # Print the key-value pair
    logger.info(f"Use this path to in the Iceberg register command s3://{s3_bucket_name_w_metadata_to_update}/{most_recent_event}. This is the newest most recent metadata.json")

# Adjust the values of these variables before running the script
s3_bucket_name_w_metadata_to_update = '<s3 bucket name that has the Iceberg metadata that you want to update>' # ex. register-iceberg-2ut1suuihxyq 
folder_path_to_metadata = '<path to the Iceberg metadata folder in the ^ bucket>' # ex. iceberg/iceberg.db/sampledataicebergtable/metadata/ 
old_s3_bucket_name_or_path = '<name of S3 bucket or the S3 file path that you want to replace in the Iceberg metadata>' # ex. glue-iceberg-from-jars-s3bucket-2ut1suuihxyq
new_s3_bucket_name_or_path = '<when you find an instance of ^ what you want to replace it with IE. the name of the S3 bucket or file path the metadata was moved to>' # ex. register-iceberg-2ut1suuihxyq

# Set up the logger with CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)  # Logs will appear in the CloudWatch Logs stream
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler) # Add the handler to the logger

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# List objects in the specified folder
response = s3.list_objects_v2(Bucket = s3_bucket_name_w_metadata_to_update, Prefix = folder_path_to_metadata)

# Check if the response contains 'Contents' (i.e., objects exist)
if 'Contents' in response:
    for obj in response['Contents']:
        object_key = obj['Key']
        
        # Get the file extension
        file_extension = object_key.split('.')[-1]
        
        if file_extension == 'avro':
            update_avro(s3_bucket_name_w_metadata_to_update, object_key, old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
        
        elif file_extension == 'json':
            update_json(s3_bucket_name_w_metadata_to_update, object_key, old_s3_bucket_name_or_path, new_s3_bucket_name_or_path)
            
else:
    raise Exception(f"No objects found in s3://{s3_bucket_name_w_metadata_to_update}/{folder_path_to_metadata}")
    
# Print the file name of the most recent metadata.json file. This can be used as an input for the register command
find_most_recent_metadata_json(s3_bucket_name_w_metadata_to_update, folder_path_to_metadata)

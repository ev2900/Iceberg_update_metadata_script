# Python script to update S3 file paths in Iceberg metadata

<img width="275" alt="map-user" src="https://img.shields.io/badge/cloudformation template deployments-00-blue"> <img width="85" alt="map-user" src="https://img.shields.io/badge/views-000-green"> <img width="125" alt="map-user" src="https://img.shields.io/badge/unique visits-000-green">

When you create an Apache Iceberg table on S3 the Iceberg table has both data files and metadata files. If you physically copy the files that make an Iceberg table to another S3 bucket the metadata files need to be updated. 

The metadata files (metadata.json files and AVRO files) have fields that reference the S3 path of the AVRO and data files. When you copy the files that make an Iceberg table to another S3 bucket the S3 path references will still be to the old / S3 bucket the files were copied from.

For example, I have an Iceberg table in S3 bucket A. I copy the data files and metadatafiles from bucket A to bucket B. The metadata.json files and AVRO contain references to S3 bucket A. We need to update these to bucket B since this Iceberg table is now stored / was copied to S3 bucket B. 

After we updated the S3 references we can optionally [register](https://github.com/ev2900/Iceberg_Glue_register_table) the updated metadata.json as a new Glue data catalog entry. An example of using the ```register_table``` command with AWS Glue is avaiable in the [Iceberg_Glue_register_table](https://github.com/ev2900/Iceberg_Glue_register_table) repository.
    
## Example using Glue python shell job

Launch the CloudFormation stack below to deploy a Glue python shell script that can be used to update the metadata.json and AVRO files.

[![Launch CloudFormation Stack](https://sharkech-public.s3.amazonaws.com/misc-public/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=iceberg-update-metadata&templateURL=https://sharkech-public.s3.amazonaws.com/misc-public/iceberg_update_metadata_script.yaml)

After you deploy the CloudFormation stack. You need to update a section of python script. Navigate to the [Glue console](https://us-east-1.console.aws.amazon.com/glue/home) click on **ETL jobs**, then select the **Update Iceberg Metadata**, then click on the **Actions** drop down then **Edit jobs**

<img width="800" alt="quick_setup" src="https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/README/glue_console.png">

# Python script to update S3 file paths in Iceberg metadata

<img width="275" alt="map-user" src="https://img.shields.io/badge/cloudformation template deployments-101-blue"> <img width="85" alt="map-user" src="https://img.shields.io/badge/views-1684-green"> <img width="125" alt="map-user" src="https://img.shields.io/badge/unique visits-632-green">

> [!CAUTION]
> As of Apache Iceberg version 1.9.0 a procedure ```rewrite_table_path``` was added. This procedure will updated the absolute path reference in the Iceberg metadata and stage this updated copy in preparation for table migration.
>
> I STRONGLY encourage you to use the ```rewrite_table_path``` procedure instead of this scripts in the repository. The documentation for this procedure can be found [HERE](https://iceberg.apache.org/docs/1.9.0/spark-procedures/#rewrite_table_path).

When you create an Apache Iceberg table on S3 the Iceberg table has both data files and metadata files. If you physically copy the files that make an Iceberg table to another S3 bucket the metadata files need to be updated.

The metadata files (metadata.json files and AVRO files) have fields that reference the S3 path of the AVRO and data files. When you copy the files that make an Iceberg table to another S3 bucket the S3 path references will still be to the old / S3 bucket the files were copied from.

For example, I have an Iceberg table in S3 bucket A. I copy the data files and metadatafiles from bucket A to bucket B. The metadata.json files and AVRO contain references to S3 bucket A. We need to update these to bucket B since this Iceberg table is now stored / was copied to S3 bucket B.

After we updated the S3 references we can optionally [register](https://github.com/ev2900/Iceberg_Glue_register_table) the updated metadata.json as a new Glue data catalog entry. An example of using the ```register_table``` command with AWS Glue is avaiable in the [Iceberg_Glue_register_table](https://github.com/ev2900/Iceberg_Glue_register_table) repository.

## Example using Glue python shell job

Launch the CloudFormation stack below to deploy a Glue python shell script that can be used to update the metadata.json and AVRO files.

[![Launch CloudFormation Stack](https://sharkech-public.s3.amazonaws.com/misc-public/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=iceberg-update-metadata&templateURL=https://sharkech-public.s3.amazonaws.com/misc-public/iceberg_update_metadata_script.yaml)

After you deploy the CloudFormation stack. You need to update a section of python script. Navigate to the [Glue console](https://us-east-1.console.aws.amazon.com/glue/home) click on **ETL jobs**, then select the **Update Iceberg Metadata**, then click on the **Actions** drop down then **Edit jobs**

<img width="800" alt="quick_setup" src="https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/README/glue_console.png">

In the Glue python script, you need to configure 4 python variables.

```
# Adjust the values of these variables before running the script
s3_bucket_name_w_metadata_to_update = '<s3 bucket name that has the Iceberg metadata that you want to update>' # ex. register-iceberg-2ut1suuihxyq
folder_path_to_metadata = '<path to the Iceberg metadata folder in the ^ bucket>' # ex. iceberg/iceberg.db/sampledataicebergtable/metadata/
old_s3_bucket_name_or_path = '<name of S3 bucket or the S3 file path that you want to replace in the Iceberg metadata>' # ex. glue-iceberg-from-jars-s3bucket-2ut1suuihxyq
new_s3_bucket_name_or_path = '<when you find an instance of ^ what you want to replace it with IE. the name of the S3 bucket or file path the metadata was moved to>' # ex. register-iceberg-2ut1suuihxyq
```

After updating these variables click on the **Save** and then **Run** button.

<img width="800" alt="quick_setup" src="https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/README/save_run.png">

If you are running this script and updating the S3 references in the metadata.json and AVRO files with the intent of using the [register_table](https://github.com/ev2900/Iceberg_Glue_register_table) command.

The python script outputs the path of the latest metadata.json file for the Iceberg table. This can be directly input into the [register_table](https://github.com/ev2900/Iceberg_Glue_register_table) command.

To find this output access the Cloudwatch Output logs for the Glue job run.

<img width="800" alt="quick_setup" src="https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/README/output_logs_1.png">

If you navigate to the end of the log stream you will see a log message that provides the file path you can use with [register_table](https://github.com/ev2900/Iceberg_Glue_register_table)

<img width="800" alt="quick_setup" src="https://github.com/ev2900/Iceberg_update_metadata_script/blob/main/README/register_path_logs.png">

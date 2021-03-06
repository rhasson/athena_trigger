## Amazon Athena CTAS and SaveAs

Ever wondered what can be done when you join SQL query parsing with the simple query output ?

Well, this Lamba function is intended to show what can be accomplished by supplementing custom commands in Amazon Athena SQL queries
with the output result file of the query in question.

I took two common features that demonstrate this.

## Create Table As Select
CTAS is a simple SQL process that creates a new table from the result of another query.
To demonstrate this we will annotate the query with --CTAS command followed by a database name, a period and a table name.

i.e. --CTAS default.test_table

On the next set of lines will be our SQL query.

Behind the scences what the Lambda function does is parse the query string for the --CTAS command and extracts the database and table names.
Then it reads the output CSV file generated by the Athena query.  It then converts it to Parquet and creates a new table in Glue Data Catalog so it's
quickly accessible for the user.

## Save As
What if you wanted to save your output file in JSON as opposed to CSV ?
Well the --SAVEAS command allows you to do just that.  The --SAVEAS command is followed by either "json" or "parquet" depending on the output format you want.
Then include the complete S3 path where to save the output.  You don't need to include a file name, that will be automatically created for you.

i.e. --SAVEAS json s3://test-bucket/some_path

## Building Lambda deployment package:
```bash
sudo yum install gcc gcc-c++ git
pip install cython
virtualenv pandas
source pandas/bin/activate
cd pandas
pip install git+https://github.com/pandas-dev/pandas.git -t packages
pip install setuptools -t packages
pip install pyarrow==0.9.0 -t packages
pip install s3fs==0.1.4 -t packages

cd packages
zip -r9 ctas_package.zip *
```

## IAM Role:
LambdaExRole needs Athena and Glue Data Catalog permissions - TODO: figure out minimal required permissions

## Create Lambda function:
```bash
aws --profile royon lambda create-function \
--region us-east-1 \
--function-name AthenaCtas \
--code S3Bucket=royon-spark,S3Key=ctas_package.zip \
--role arn:aws:iam::112233445566:role/LambdaExRole \
--handler index.handler \
--runtime python2.7 \
--timeout 100 \
--memory-size 2048 \
--environment '{"Variables":{"data_output_bucket":"royon-demo", "data_output_path":"athena_query_ctas", "manifest_output_bucket":"royon-demo", "manifest_output_path":"athena_query_manifest", "table_prefix":"ctas_", "database":"default"}}'
```

## S3 trigger:
Add S3 trigger on to the Athean query output S3 bucket.  Set a file filter on ".csv" so that we only trigger on query result files.

```bash
aws --profile royon lambda add-permission \
--region us-east-1 \
--function-name AthenaCtas \
--statement-id 11223344 \
--action "lambda:InvokeFunction" \
--principal s3.amazonaws.com \
--source-arn "arn:aws:s3:::aws-athena-query-results-us-east-1-112233445566" \
--source-account 112233445566
```

Note: Only the creator of the bucket is allowed to set this bucket policy.

```bash
aws --profile royon s3api put-bucket-notification-configuration \
--region us-east-1 \
--bucket aws-athena-query-results-us-east-1-112233445566 \
--notification-configuration '{"LambdaFunctionConfigurations": [{"Filter": {"Key": {"FilterRules": [{"Name": "Prefix", "Value": "Unsaved/"}, {"Name": "Suffix", "Value": ".csv"}]}}, "LambdaFunctionArn": "arn:aws:lambda:us-east-1:112233445566:function:AthenaCtas", "Events": ["s3:ObjectCreated:Put"]}]}'
```

## Usage:
As the first line of your Athena query:

--CTAS dbname.tablename
--CTAS tablename --> uses database environment variable or "default" database

--SAVEAS json s3://bucket/path
--SAVEAS parquet s3://bucket/path

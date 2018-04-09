import os
import re
import sys
import json
import boto3
import tempfile
import pandas as pd
import pyarrow as pa

DATA_OUTPUT_BUCKET = os.environ['data_output_bucket'].strip('/')
DATA_OUTPUT_PATH = os.environ['data_output_path'].strip('/')
MANIFEST_OUTPUT_BUCKET = os.environ['manifest_output_bucket'].strip('/')
MANIFEST_OUTPUT_PATH = os.environ['manifest_output_path'].strip('/')
TABLE_PREFIX = os.environ.get('table_prefix', "")
DATABASE = os.environ.get('database', "default")

TABLE_PROP = {
  "Name": "",
  "StorageDescriptor": {
    "Columns": None,
    "Location": "",
    "InputFormat": "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "Compressed": True,
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "Parameters": {
        "serialization.format": "1"
      }
    }
  },
  "PartitionKeys": [
    {
      "Type": "string", 
      "Name": "qid"
    }
  ],
  "Parameters": {
    "EXTERNAL": "TRUE"
  },
  "TableType": "EXTERNAL_TABLE"
}

PARTITION_SPEC = {
  "StorageDescriptor": {
    "Columns": None,
    "Location": "",
    "InputFormat": "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "Compressed": True,
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "Parameters": {
        "serialization.format": "1"
      }
    }
  },
  "Values": []
}

def types(dtype):
  return {
    'float64': 'double',
    'int64': 'bigint',
    'datetime': 'timestamp',
    'unicode': 'string'
  }.get(dtype, 'string')

def createPyarrowSchema(table):
  out = []
  for i in table.itercolumns():
    if not re.search('__index_level_', i.name):
      out.append({ "Name": i.name, "Type": types(str(i.type)) })
  return out

def writeJson(dst, data):
  from urlparse import urlparse
  import gzip

  path = urlparse(dst)
  tfile = tempfile.NamedTemporaryFile(delete=True)
  gzf = gzip.GzipFile(mode="wb", fileobj=tfile)
  gzf.write(data)
  gzf.flush()
  gzf.close()
  tfile.seek(0)
  s3 = boto3.client('s3')
  s3.upload_file(tfile.name, path.netloc, path.path.strip('/'))
  tfile.close()

def analyzeQuery(filename):
  try:
    athena = boto3.client('athena')
    res = athena.get_query_execution(QueryExecutionId=filename)
    
    ctas = re.search("^--CTAS ([\w\-]+)\.?([\w\-]+)?", res['QueryExecution']['Query'])
    saveas = re.search("^--SAVEAS (json|parquet) ([\w\/\:\-\.\=]+)", res['QueryExecution']['Query'])
    
    if ctas is not None:
      d, t = ctas.groups()
      print("CTAS - {} : {}".format(d, t))
      if t is None:
        tablename = d
        db = DATABASE
      else:
        tablename = t
        db = d
      return {"type": "ctas", "table": tablename, "database": db}
    elif saveas is not None:
      f, p = saveas.groups()
      return {"type": "saveas", "format": f, "path": p}
    else:
      sys.exit(0)
  except Exception as err:
    print("Failed to get query detail.  Exception: {}".format(err))
    sys.exit(1)

def readWriteData(fmt, src_path, dst_path):
  try:
    data = pd.read_csv(src_path, encoding='utf-8')

    if fmt == 'parquet':
      table = pa.Table.from_pandas(data)
      schema = createPyarrowSchema(table)
      data.to_parquet(dst_path, engine="pyarrow", compression="snappy", **{"flavor":"spark"})
      return schema
    elif fmt == 'json':
      writeJson(dst_path, data.to_json(orient='records'))
      return
    else:
      raise Exception("No output format provided. Exiting")

  except Exception as err:
    print("Failed to convert CSV to {}. Exception: {}".format(fmt.title(), err))
    sys.exit(1)
  else:
    print("Successfully converted to {} and saved file to {}".format(fmt.title(), dst_path))

def writeManifest(data_file_path, filename):
  # write manifest file
  try:
    tfile = tempfile.NamedTemporaryFile(delete=True)
    tfile.write(data_file_path)
    tfile.seek(0)
  
    s3 = boto3.client('s3')
    s3.upload_file(tfile.name, MANIFEST_OUTPUT_BUCKET, "{path}/qid={qid}/{name}".format(path=MANIFEST_OUTPUT_PATH, qid=filename, name="manifest.json"))
  except Exception as err:
    print("Failed to write manifest file to S3. Exception: {}".format(err))
    sys.exit(1)
  else:
    print("Successfully saved manifest file")
  finally:
    tfile.close()

def updateDataCatalog(schema, tablename, dbname, filename):
  # create a new table in Data Catalog
  TABLE_PROP['Name'] = TABLE_PREFIX + tablename
  TABLE_PROP['StorageDescriptor']['Columns'] = schema
  TABLE_PROP['StorageDescriptor']['Location'] = "s3://{bucket}/{path}/hive".format(bucket=MANIFEST_OUTPUT_BUCKET, path=MANIFEST_OUTPUT_PATH)

  PARTITION_SPEC['StorageDescriptor']['Columns'] = schema
  PARTITION_SPEC['StorageDescriptor']['Location'] = "s3://{bucket}/{path}/qid={qid}".format(bucket=MANIFEST_OUTPUT_BUCKET, path=MANIFEST_OUTPUT_PATH, qid=filename)
  PARTITION_SPEC['Values'] = [ filename ]

  try:
    glue = boto3.client('glue')
    glue.create_table(DatabaseName=dbname, TableInput=TABLE_PROP)
    glue.create_partition(DatabaseName=dbname, TableName=TABLE_PROP['Name'], PartitionInput=PARTITION_SPEC)
  except Exception as err:
    print("Failed to create table.  Exception: {}".format(err))
    sys.exit(1)
  else:
    print("Table {} was successfully created in database {}".format(TABLE_PROP['Name'], dbname))

def handler(event, context):
  record = event['Records'][0]
  bucket = record['s3']['bucket']['name']
  key = record['s3']['object']['key']
 
  print("Triggered file: {bucket}/{key}".format(bucket=bucket,key=key))

  try:
    filename = os.path.basename(key).split('.')[0]
    ret = analyzeQuery(filename)

    if ret['type'] == "ctas":
      src_path = "s3://{bucket}/{key}".format(bucket=bucket, key=key)
      dst_path = "s3://{bucket}/{path}/{name}".format(bucket=DATA_OUTPUT_BUCKET, path=DATA_OUTPUT_PATH, name=filename + ".snappy.parquet")
      schema = readWriteData("parquet", src_path, dst_path)
      writeManifest(dst_path, filename)
      updateDataCatalog(schema, ret['table'], ret['database'], filename)
    elif ret['type'] == "saveas":
      src_path = "s3://{bucket}/{key}".format(bucket=bucket, key=key)
      if ret['format'] == 'parquet':
        dst_path = ret['path'].strip('/') + "/{name}".format(name=filename + ".snappy.parquet")
        readWriteData("parquet", src_path, dst_path)
      elif ret['format'] == 'json':
        dst_path = ret['path'].strip('/') + "/{name}".format(name=filename + ".json.gz")
        readWriteData("json", src_path, dst_path)
      else:
        raise Exception("Failed to save output file")
    else:
      raise Exception("Did not find anything to do")
  except Exception as err:
    print("Error: {}".format(err))
    sys.exit(1)

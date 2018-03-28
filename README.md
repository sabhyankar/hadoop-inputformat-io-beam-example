# Using Apache Beam's HadoopInputFormatIO to read files in Google Cloud Storage

*An example Apache Beam pipeline that reads Orc formatted files from Google Cloud Storage using HadoopInputFormatIO and writes Avro GenericRecord objects using AvroIO*

[Apache Beam](https://beam.apache.org/) provides various [built in I/O transforms](https://beam.apache.org/documentation/io/built-in/) to access data easily from various sources and sinks.

One such transform is the [HadoopInputFormatIO transform](https://beam.apache.org/documentation/io/built-in/hadoop/) that allows us to access *any* data source that implements Hadoop's InputFormat.

This sample pipeline demonstrates the use of HadoopInputFormatIO to read [Apache Orc](https://orc.apache.org/) formatted files from Google Cloud Storage. Apache Orc provides a convenient [OrcInputFormat](https://orc.apache.org/docs/mapreduce.html) that returns [OrcStruct](https://orc.apache.org/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcStruct.html) values along with a NullWritable key. 

The pipeline converts the OrcStruct to [Avro GenericRecord](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/generic/GenericData.Record.html) objects that can be written back to Google Cloud Storage using the built-in AvroIO transform.

##### The pipeline inputs are:
+ Google Cloud Storage (GCS) bucket containing the Orc formatted files
+ GCS bucket to store the output Avro files
+ GCS location of the target Avro Schema file

In order to access GCS using the OrcInputFormat, we would need to create a Google Cloud Service Account with permissions to read from the GCS buckets that contain the Orc formatted files and the Avro schema file.

##### Creating a service account:
```
gcloud iam service-accounts create hadoop-io-example-svc --display-name "hadoop-io-example service account"
Created service account [hadoop-io-example-svc].
```

##### Granting roles to the service account:
```
gcloud projects add-iam-policy-binding <gcp-project-id> \
--member=serviceAccount:hadoop-io-example-svc@<gcp-project-id>.iam.gserviceaccount.com \
--role=roles/storage.admin

gcloud projects add-iam-policy-binding <gcp-project-id> \
--member=serviceAccount:hadoop-io-example-svc@<gcp-project-id>.iam.gserviceaccount.com \
--role=roles/dataflow.admin
```

##### Extract the service account credential json:
```
gcloud iam service-accounts keys create hadoop-io-example-svc.json \
--iam-account=hadoop-io-example-svc@<gcp-project-id>.iam.gserviceaccount.com
```

##### Executing the pipeline:
```
export GOOGLE_APPLICATION_CREDENTIALS=<path-to-service-account-credential-json>
mvn compile exec:java -Dexec.mainClass=com.google.cloud.pso.HadoopInputFormatIOExample \
-Dexec.args="--runner=DataflowRunner \
--project=<gcp-project-id> --stagingLocation=gs://<gcp-staging-bucket> \
--inputDir=gs://<input-bucket> \
--outputDir=gs://<output-bucket> \
--schemaFile=gs://<schema-bucket>/<avro-schema-file-avsc>"
```


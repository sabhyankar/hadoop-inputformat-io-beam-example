/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.pso;

import com.google.cloud.pso.configuration.HadoopConfiguration;
import com.google.cloud.pso.transformation.OrcStructToAvroFn;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * An example pipeline that uses HadoopInputFormatIO to read files from GCS using a
 * HadoopInputFormat class.
 *
 * <p>In this example class, {@link HadoopInputFormatIO}, we will:
 *
 * <pre>
 *     1. Read Orc files (in GCS) using the OrcInputFormat via HadoopInputFormatIO Beam transform
 *     2. Convert OrcStruct to Avro GenericRecord
 *     3. Write Avro GenericRecords to GCS via AvroIO Beam transform
 * </pre>
 *
 * The pipeline would need the following required flags: --runner=DataflowRunner
 * --project=project-id --stagingLocation=gcs-staging-location
 * --inputDir=gcs-input-directory-with-orc-files --outputDir=gcs-output-directory-with-avro-files
 * --schemaFile=gcs-avro-schema-file-location
 *
 * <p>In addition the pipeline requires either application-default credentials or the
 * GOOGLE_APPLICATION_CREDENTIALS to be set to a service account json key. The service account
 * should have the required read privileges on the buckets for the source files and schema.
 *
 * @see <a
 *     href="https://orc.apache.org/api/orc-mapreduce/index.html?org/apache/orc/mapreduce/OrcInputFormat.html">OrcInputFormat</a>)
 */
public class HadoopInputFormatIOExample {

  private static final String INPUT_FORMAT_KEY = "mapreduce.job.inputformat.class";

  private static final String KEY_CLASS_KEY = "key.class";

  private static final String VALUE_CLASS_KEY = "value.class";

  private static final String INPUT_DIR_KEY = "mapreduce.input.fileinputformat.inputdir";

  private static final String OPTIONAL_PREFIX = "output";

  private interface Options extends DataflowPipelineOptions {

    @Description("GCS path for Orc files (input)")
    @Validation.Required
    String getInputDir();

    void setInputDir(String inputDir);

    @Description("GCS path for Avro files (output)")
    @Validation.Required
    String getOutputDir();

    void setOutputDir(String outputDir);

    @Description("Schema file GCS location")
    @Validation.Required
    String getSchemaFile();

    void setSchemaFile(String schemaFile);
  }

  /**
   * Helper method to extract an Avro Schema object from the json file in GCS.
   *
   * @param options Options to extract schema file GCS path from
   * @return {@link Schema} Avro schema object from the json schema
   * @throws IOException
   */
  private static Schema getSchemaFromPath(Options options) throws IOException {

    GcsUtil gcsUtil = options.getGcsUtil();
    GcsPath gcsPath = GcsPath.fromUri(options.getSchemaFile());

    int bufferSize = (int) gcsUtil.fileSize(gcsPath);
    String schemaString;
    try (SeekableByteChannel sbc = gcsUtil.open(gcsPath)) {
      ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
      sbc.read(buffer);
      buffer.flip();
      schemaString = new String(buffer.array(), StandardCharsets.UTF_8);
    }

    return new Schema.Parser().parse(schemaString);
  }

  private static String getOutputPathWithPrefix(Options options) {
    String outputPath = options.getOutputDir();
    ResourceId outputResourceId = FileBasedSink.convertToFileResourceIfPossible(outputPath);
    return (outputResourceId.isDirectory()) ? outputPath + OPTIONAL_PREFIX : outputPath;
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  private static PipelineResult run(Options options) throws IOException {

    Pipeline pipeline = Pipeline.create(options);

    Schema schema = getSchemaFromPath(options);

    pipeline.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(schema));

    Configuration hadoopConf = HadoopConfiguration.get(options);

    // OrcInputFormat requires the following 4 properties to be set
    hadoopConf.setClass(INPUT_FORMAT_KEY, OrcInputFormat.class, InputFormat.class);

    hadoopConf.setClass(KEY_CLASS_KEY, NullWritable.class, Object.class);

    hadoopConf.setClass(VALUE_CLASS_KEY, OrcStruct.class, Object.class);

    hadoopConf.set(INPUT_DIR_KEY, options.getInputDir());

    // A simple function to convert an OrcStruct object to a GenericRecord object
    SimpleFunction<OrcStruct, GenericRecord> simpleFunction =
        new OrcStructToAvroFn(schema.toString());

    String resolvedOutputPath = getOutputPathWithPrefix(options);

    pipeline
        .apply(
            "Read Orc files",
            HadoopInputFormatIO.<NullWritable, GenericRecord>read()
                .withConfiguration(hadoopConf)
                .withValueTranslation(simpleFunction))
        .apply("Extract GenericRecord Values", Values.create())
        .apply(
            "Write GenericRecords to Avro files",
            AvroIO.writeGenericRecords(schema)
                .withCodec(CodecFactory.snappyCodec())
                .to(resolvedOutputPath));

    return pipeline.run();
  }
}

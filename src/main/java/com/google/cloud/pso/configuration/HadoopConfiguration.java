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
package com.google.cloud.pso.configuration;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * A helper class {@link HadoopConfiguration} that configures a Hadoop Configuration object {@link
 * Configuration} with the GCS specific properties required for accessing objects stored in Google
 * Cloud Storage.
 *
 * <p>HadoopInputFormatIO uses an InputFormat that needs to access files in GCS This access is via
 * GCS connector that requires a properly configured Hadoop configuration object
 */
public class HadoopConfiguration {

  private static final String FS_GS_IMPL_DEFAULT =
      "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  private static final String FS_ABS_GS_IMPL_DEFAULT =
      "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS";
  private static final Integer FS_GS_BLOCK_SZ_DEFAULT = 67108864; // 64 MB

  public static Configuration get(DataflowPipelineOptions options) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.gs.impl", FS_GS_IMPL_DEFAULT);
    conf.set("fs.AbstractFileSystem.gs.impl", FS_ABS_GS_IMPL_DEFAULT);
    conf.set("fs.gs.project.id", options.getProject());
    conf.setBoolean("google.cloud.auth.service.account.enable", true);
    conf.setInt("fs.gs.block.size", FS_GS_BLOCK_SZ_DEFAULT);

    String serviceAccountId = getServiceAccountId();
    if (serviceAccountId == null)
      throw new IllegalArgumentException(
          "Missing service account: "
              + "Either set environment variable GOOGLE_APPLICATION_CREDENTIALS or "
              + "application-default via gcloud.");

    conf.set("google.cloud.auth.service.account.email", getServiceAccountId());

    return conf;
  }

  private static String getServiceAccountId() throws IOException {

    return GoogleCredential.getApplicationDefault().getServiceAccountId();
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.processing.loading.jsoninput.JsonInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

@InterfaceAudience.User
@InterfaceStability.Evolving
public class JsonReaderBuilder {

  private String filePath;
  // to identify records when one row json data is present in multi lines
  private String recordIdentifier;

  /**
   * Construct a JsonReaderBuilder with table path and table name
   *
   * @param filePath table path
   */
  JsonReaderBuilder(String filePath) {
    Objects.requireNonNull(filePath);
    this.filePath = filePath;
  }

  /**
   *
   * @param filePath
   * @param recordIdentifier to identify the records in multiline json
   */
  JsonReaderBuilder(String filePath, String recordIdentifier) {
    Objects.requireNonNull(filePath);
    Objects.requireNonNull(recordIdentifier);
    this.filePath = filePath;
    this.recordIdentifier = recordIdentifier;
  }

  /**
   * Set the access key for S3
   *
   * @param key   the string of access key for different S3 type,like: fs.s3a.access.key
   * @param value the value of access key
   * @return JsonReaderBuilder object
   */
  public JsonReaderBuilder setAccessKey(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the access key for S3.
   *
   * @param value the value of access key
   * @return JsonReaderBuilder object
   */
  public JsonReaderBuilder setAccessKey(String value) {
    return setAccessKey(Constants.ACCESS_KEY, value);
  }

  /**
   * Set the secret key for S3
   *
   * @param key   the string of secret key for different S3 type,like: fs.s3a.secret.key
   * @param value the value of secret key
   * @return JsonReaderBuilder object
   */
  public JsonReaderBuilder setSecretKey(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the secret key for S3
   *
   * @param value the value of secret key
   * @return JsonReaderBuilder object
   */
  public JsonReaderBuilder setSecretKey(String value) {
    return setSecretKey(Constants.SECRET_KEY, value);
  }

  /**
   * Set the endpoint for S3
   *
   * @param key   the string of endpoint for different S3 type,like: fs.s3a.endpoint
   * @param value the value of endpoint
   * @return JsonReaderBuilder object
   */
  public JsonReaderBuilder setEndPoint(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the endpoint for S3
   *
   * @param value the value of endpoint
   * @return JsonReaderBuilder object
   */
  public JsonReaderBuilder setEndPoint(String value) {
    return setEndPoint(Constants.ENDPOINT, value);
  }

  /**
   * Build JsonReader
   *
   * @param <T>
   * @return JsonReader
   * @throws IOException
   * @throws InterruptedException
   */
  public <T> JsonReader<T> build() throws IOException, InterruptedException {
    final JsonInputFormat format = new JsonInputFormat();
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(FileInputFormat.INPUT_DIR, filePath);
    hadoopConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    final Job job = new Job(hadoopConf);
    if (recordIdentifier == null) {
      format.setOneRecordPerLine(job, true);
    } else {
      format.setRecordIdentifier(job, recordIdentifier);
    }

    try {
      final List<InputSplit> splits =
          format.getSplits(job);

      boolean foundJsonFile = false;
      FileSplit fileSplit;
      List<RecordReader<Void, T>> readers = new ArrayList<>(splits.size());
      for (InputSplit split : splits) {
        fileSplit = (FileSplit) split;
        if (fileSplit.getPath().toString().endsWith(".json")) {
          foundJsonFile = true;
          TaskAttemptContextImpl attempt =
              new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
          RecordReader reader = format.createRecordReader(split, attempt);
          try {
            reader.initialize(split, attempt);
            readers.add(reader);
          } catch (Exception e) {
            reader.close();
            throw e;
          }
        }
      }
      if (!foundJsonFile) {
        throw new IOException("json file not found in the path" + filePath);
      }
      return new JsonReader<>(readers);
    } catch (Exception ex) {
      throw ex;
    }
  }
}

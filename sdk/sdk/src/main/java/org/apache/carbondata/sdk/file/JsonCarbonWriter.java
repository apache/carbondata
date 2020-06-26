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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeInmemoryMergeHolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Writer Implementation to write Json Record to carbondata file.
 * json writer requires the path of json file and carbon schema.
 */
@InterfaceAudience.User
public class JsonCarbonWriter extends CarbonWriter {
  private Configuration configuration;
  private RecordWriter<NullWritable, ObjectArrayWritable> recordWriter;
  private TaskAttemptContext context;
  private ObjectArrayWritable writable;
  private CarbonFile[] dataFiles;
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(UnsafeInmemoryMergeHolder.class.getName());

  JsonCarbonWriter(CarbonLoadModel loadModel, Configuration configuration) throws IOException {
    CarbonTableOutputFormat.setLoadModel(configuration, loadModel);
    CarbonTableOutputFormat outputFormat = new CarbonTableOutputFormat();
    JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
    Random random = new Random();
    TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
    TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(configuration, attemptID);
    this.recordWriter = outputFormat.getRecordWriter(context);
    this.context = context;
    this.writable = new ObjectArrayWritable();
    this.configuration = configuration;
  }

  @Override
  public void setDataFiles(CarbonFile[] dataFiles) throws IOException {
    if (dataFiles == null || dataFiles.length == 0) {
      throw new RuntimeException("data files can't be empty.");
    }
    Reader jsonReader = null;
    for (CarbonFile dataFile : dataFiles) {
      try {
        jsonReader = this.buildJsonReader(dataFile, this.configuration);
        new JSONParser().parse(jsonReader);
      } catch (FileNotFoundException ex) {
        throw new FileNotFoundException("File " + dataFile + " not found to build carbon writer.");
      } catch (ParseException ex) {
        throw new RuntimeException("File " + dataFile + " is not in json format.");
      } finally {
        if (jsonReader != null) {
          jsonReader.close();
        }
      }
    }
    this.dataFiles = dataFiles;
  }

  private java.io.Reader buildJsonReader(CarbonFile file, Configuration conf)
      throws IOException {
    InputStream inputStream = FileFactory.getDataInputStream(file.getPath(), -1, conf);
    java.io.Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    return reader;
  }

  /**
   * Write single row data, accepts one row of data as json string
   *
   * @param object (json row as a string)
   * @throws IOException
   */
  @Override
  public void write(Object object) throws IOException {
    Objects.requireNonNull(object, "Input cannot be null");
    try {
      String[] jsonString = new String[1];
      jsonString[0] = (String) object;
      writable.set(jsonString);
      recordWriter.write(NullWritable.get(), writable);
    } catch (Exception e) {
      close();
      throw new IOException(e);
    }
  }

  /**
   * Flush and close the writer
   */
  @Override
  public void close() throws IOException {
    try {
      recordWriter.close(context);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void loadSingleFile(CarbonFile file) throws IOException {
    Reader reader = null;
    try {
      reader = this.buildJsonReader(file, configuration);
      JSONParser jsonParser = new JSONParser();
      Object jsonRecord = jsonParser.parse(reader);
      if (jsonRecord instanceof JSONArray) {
        JSONArray jsonArray = (JSONArray) jsonRecord;
        for (Object record : jsonArray) {
          this.write(record.toString());
        }
      } else {
        this.write(jsonRecord.toString());
      }
    } catch (ParseException ex) {
      LOGGER.error(ex);
      throw new IOException(ex.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Load data of all or selected json files at given location iteratively.
   *
   * @throws IOException
   */
  @Override
  public void write() throws IOException {
    if (this.dataFiles == null || this.dataFiles.length == 0) {
      throw new RuntimeException("'withJsonPath()' must be called to support load json files");
    }
    for (CarbonFile dataFile : this.dataFiles) {
      this.loadSingleFile(dataFile);
    }
  }
}

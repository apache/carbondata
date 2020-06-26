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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * Implementation to write rows in CSV format to carbondata file.
 */
@InterfaceAudience.Internal
class CSVCarbonWriter extends CarbonWriter {

  private RecordWriter<NullWritable, ObjectArrayWritable> recordWriter;
  private TaskAttemptContext context;
  private ObjectArrayWritable writable;
  public CsvParser csvParser = null;
  private boolean skipHeader = false;
  private String filePath = "";
  private boolean isDirectory = false;
  private List<String> fileList;

  CSVCarbonWriter(CarbonLoadModel loadModel, Configuration hadoopConf) throws IOException {
    CarbonTableOutputFormat.setLoadModel(hadoopConf, loadModel);
    CarbonTableOutputFormat format = new CarbonTableOutputFormat();
    JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
    Random random = new Random();
    TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
    TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(hadoopConf, attemptID);
    this.recordWriter = format.getRecordWriter(context);
    this.context = context;
    this.writable = new ObjectArrayWritable();
  }

  CSVCarbonWriter() { }

  public void setSkipHeader(boolean skipHeader) {
    this.skipHeader = skipHeader;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public void setIsDirectory(boolean isDirectory) {
    this.isDirectory = isDirectory;
  }

  public void setFileList(List<String> fileList) {
    this.fileList = fileList;
  }

  /**
   * Write single row data, input row is of type String[]
   */
  @Override
  public void write(Object object) throws IOException {
    try {
      writable.set((Object[]) object);
      recordWriter.write(NullWritable.get(), writable);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Load data of all or selected csv files at given location iteratively.
   *
   * @throws IOException
   */
  @Override
  public void write() throws IOException {
    if (this.filePath.length() == 0) {
      throw new RuntimeException("'withCsvPath()' must be called to support load files");
    }
    CsvParserSettings settings = CSVInputFormat.extractCsvParserSettings(new Configuration());
    this.csvParser = new CsvParser(settings);
    if (!this.isDirectory) {
      this.loadSingleFile(new File(this.filePath));
    } else {
      if (this.fileList == null || this.fileList.size() == 0) {
        File[] dataFiles = new File(this.filePath).listFiles();
        if (dataFiles == null || dataFiles.length == 0) {
          throw new RuntimeException("No CSV file found at given location. Please provide" +
              "the correct folder location.");
        }
        Arrays.sort(dataFiles);
        for (File dataFile : dataFiles) {
          this.loadSingleFile(dataFile);
        }
      } else {
        for (String file : this.fileList) {
          this.loadSingleFile(new File(this.filePath + "/" + file));
        }
      }
    }
  }

  private void loadSingleFile(File file) throws IOException {
    this.csvParser.beginParsing(file);
    String[] row;
    boolean skipFirstRow = this.skipHeader;
    while ((row = this.csvParser.parseNext()) != null) {
      if (skipFirstRow) {
        skipFirstRow = false;
        continue;
      }
      this.write(row);
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
}

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

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
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

  private Configuration configuration;
  private RecordWriter<NullWritable, ObjectArrayWritable> recordWriter;
  private TaskAttemptContext context;
  private ObjectArrayWritable writable;
  private CsvParser csvParser = null;
  private boolean skipHeader = false;
  private CarbonFile[] dataFiles;

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
    this.configuration = hadoopConf;
  }

  public void setSkipHeader(boolean skipHeader) {
    this.skipHeader = skipHeader;
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

  private CsvParser buildCsvParser(Configuration conf) {
    CsvParserSettings settings = CSVInputFormat.extractCsvParserSettings(conf);
    return new CsvParser(settings);
  }

  @Override
  public void setDataFiles(CarbonFile[] dataFiles) throws IOException {
    if (dataFiles == null || dataFiles.length == 0) {
      throw new RuntimeException("data files can't be empty.");
    }
    DataInputStream csvInputStream = null;
    CsvParser csvParser = this.buildCsvParser(this.configuration);
    for (CarbonFile dataFile : dataFiles) {
      try {
        csvInputStream = FileFactory.getDataInputStream(dataFile.getPath(),
            -1, this.configuration);
        csvParser.beginParsing(csvInputStream);
      } catch (IllegalArgumentException ex) {
        if (ex.getCause() instanceof FileNotFoundException) {
          throw new FileNotFoundException("File " + dataFile +
              " not found to build carbon writer.");
        }
        throw ex;
      } finally {
        if (csvInputStream != null) {
          csvInputStream.close();
        }
      }
    }
    this.dataFiles = dataFiles;
  }

  /**
   * Load data of all or selected csv files at given location iteratively.
   *
   * @throws IOException
   */
  @Override
  public void write() throws IOException {
    if (this.dataFiles == null || this.dataFiles.length == 0) {
      throw new RuntimeException("'withCsvPath()' must be called to support load files");
    }
    this.csvParser = this.buildCsvParser(this.configuration);
    for (CarbonFile dataFile : this.dataFiles) {
      this.loadSingleFile(dataFile);
    }
  }

  private void loadSingleFile(CarbonFile file) throws IOException {
    DataInputStream csvDataInputStream = FileFactory
        .getDataInputStream(file.getPath(), -1, this.configuration);
    this.csvParser.beginParsing(csvDataInputStream);
    String[] row;
    boolean skipFirstRow = this.skipHeader;
    try {
      while ((row = this.csvParser.parseNext()) != null) {
        if (skipFirstRow) {
          skipFirstRow = false;
          continue;
        }
        this.write(row);
      }
    } finally {
      if (csvDataInputStream != null) {
        csvDataInputStream.close();
      }
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

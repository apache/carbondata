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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Implementation to write parquet rows in avro format to carbondata file.
 */
public class ParquetCarbonWriter extends AvroCarbonWriter {
  private AvroCarbonWriter avroCarbonWriter = null;
  private String filePath = "";
  private boolean isDirectory = false;
  private List<String> fileList;

  ParquetCarbonWriter(AvroCarbonWriter avroCarbonWriter) {
    this.avroCarbonWriter = avroCarbonWriter;
  }

  @Override
  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void setIsDirectory(boolean isDirectory) {
    this.isDirectory = isDirectory;
  }

  @Override
  public void setFileList(List<String> fileList) {
    this.fileList = fileList;
  }

  /**
   * Load data of all parquet files at given location iteratively.
   *
   * @throws IOException
   */
  @Override
  public void write() throws IOException {
    if (this.filePath.length() == 0) {
      throw new RuntimeException("'withParquetPath()' " +
          "must be called to support load parquet files");
    }
    if (this.avroCarbonWriter == null) {
      throw new RuntimeException("avro carbon writer can not be null");
    }
    if (this.isDirectory) {
      if (this.fileList == null || this.fileList.size() == 0) {
        File[] dataFiles = new File(this.filePath).listFiles();
        if (dataFiles == null || dataFiles.length == 0) {
          throw new RuntimeException("No Parquet file found at given location. Please provide " +
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
    } else {
      this.loadSingleFile(new File(this.filePath));
    }
  }

  private void loadSingleFile(File file) throws IOException {
    AvroReadSupport<GenericRecord> avroReadSupport = new AvroReadSupport<>();
    ParquetReader<GenericRecord> parquetReader = ParquetReader.builder(avroReadSupport,
        new Path(String.valueOf(file))).withConf(new Configuration()).build();
    GenericRecord genericRecord = null;
    while ((genericRecord = parquetReader.read()) != null) {
      System.out.println(genericRecord);
      this.avroCarbonWriter.write(genericRecord);
    }
  }

  /**
   * Flush and close the writer
   */
  @Override
  public void close() throws IOException {
    try {
      this.avroCarbonWriter.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}

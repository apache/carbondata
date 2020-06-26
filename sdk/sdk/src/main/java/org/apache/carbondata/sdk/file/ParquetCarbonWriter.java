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

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Implementation to write parquet rows in avro format to carbondata file.
 */
public class ParquetCarbonWriter extends CarbonWriter {
  private Configuration configuration;
  private CarbonFile[] dataFiles;
  private AvroCarbonWriter avroCarbonWriter;

  ParquetCarbonWriter(CarbonLoadModel loadModel, Configuration hadoopConf, Schema avroSchema)
      throws IOException {
    this.avroCarbonWriter = new AvroCarbonWriter(loadModel, hadoopConf, avroSchema);
    this.configuration = hadoopConf;
  }

  @Override
  public void setDataFiles(CarbonFile[] dataFiles) throws IOException {
    if (dataFiles == null || dataFiles.length == 0) {
      throw new RuntimeException("data files can't be empty.");
    }
    Schema parquetSchema = null;
    for (CarbonFile dataFile : dataFiles) {
      Schema currentFileSchema = extractParquetSchema(dataFile,
          this.configuration);
      if (parquetSchema == null) {
        parquetSchema = currentFileSchema;
      } else {
        if (!parquetSchema.equals(currentFileSchema)) {
          throw new RuntimeException("All the parquet files must be having the same schema.");
        }
      }
    }
    this.dataFiles = dataFiles;
  }

  /**
   * TO extract the parquet schema from the given parquet file
   */
  public static Schema extractParquetSchema(CarbonFile dataFile,
      Configuration configuration) throws IOException {
    ParquetReader<GenericRecord> parquetReader =
        buildParquetReader(dataFile.getPath(), configuration);
    Schema parquetSchema = parquetReader.read().getSchema();
    parquetReader.close();
    return parquetSchema;
  }

  private static ParquetReader<GenericRecord> buildParquetReader(String path, Configuration conf)
      throws IOException {
    try {
      AvroReadSupport<GenericRecord> avroReadSupport = new AvroReadSupport<>();
      return ParquetReader.builder(avroReadSupport,
          new Path(path)).withConf(conf).build();
    } catch (FileNotFoundException ex) {
      throw new FileNotFoundException("File " + path + " not found to build carbon writer.");
    }
  }

  @Override
  public void write(Object object) {
    throw new UnsupportedOperationException("Carbon doesn't " +
        "support writing a single Parquet object");
  }

  @Override
  public void close() throws IOException {
    this.avroCarbonWriter.close();
  }

  /**
   * Load data of all parquet files at given location iteratively.
   *
   * @throws IOException
   */
  @Override
  public void write() throws IOException {
    if (this.dataFiles == null || this.dataFiles.length == 0) {
      throw new RuntimeException("'withParquetPath()' " +
          "must be called to support loading parquet files");
    }
    for (CarbonFile dataFile : this.dataFiles) {
      this.loadSingleFile(dataFile);
    }
  }

  private void loadSingleFile(CarbonFile file) throws IOException {
    ParquetReader<GenericRecord> parquetReader = null;
    try {
      parquetReader = buildParquetReader(file.getPath(), this.configuration);
      GenericRecord genericRecord;
      while ((genericRecord = parquetReader.read()) != null) {
        this.avroCarbonWriter.write(genericRecord);
      }
    } finally {
      if (parquetReader != null) {
        parquetReader.close();
      }
    }
  }
}

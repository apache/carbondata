/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 */
package org.carbondata.spark.partition.api.impl;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordslogger;
import org.carbondata.spark.partition.api.DataPartitioner;
import org.carbondata.spark.partition.api.Partition;
import org.carbondata.spark.partition.reader.CSVParser;
import org.carbondata.spark.partition.reader.CSVReader;
import org.carbondata.spark.partition.reader.CSVWriter;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.execution.command.Partitioner;

/**
 * Split the CSV file into the number of partitions using the given partition information
 */
public class CSVFilePartitioner {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CSVFilePartitioner.class.getName());
  private String partitionerClass;
  private String sourceFilesBasePath;
  private boolean partialSuccess;
  /**
   * badRecordslogger
   */
  private BadRecordslogger badRecordslogger;

  /**
   * @param partitionerClass
   */
  public CSVFilePartitioner(String partitionerClass, String sourceFilesBasePath) {
    this.partitionerClass = partitionerClass;
    this.sourceFilesBasePath = sourceFilesBasePath;
  }

  public boolean isPartialSuccess() {
    return partialSuccess;
  }

  /**
   * @param sourceFilePath - Source raw data file in local disk
   * @param targetFolder   - Target folder to save the partitioned files
   * @param nodes
   * @param properties
   * @param i
   */
  @Deprecated public void splitFile(String schemaName, String cubeName, List<String> sourceFilePath,
      String targetFolder, List<String> nodes, int partitionCount, String[] partitionColumn,
      String[] requiredColumns, String delimiter, String quoteChar, String fileHeader,
      String escapeChar, boolean multiLine) throws Exception {
    LOGGER
        .info("Processing file split: " + sourceFilePath);

    // Create the target folder
    FileFactory.mkdirs(targetFolder, FileFactory.getFileType(targetFolder));

    String[] headerColumns = null;

    HashMap<Partition, CSVWriter> outputStreamsMap =
        new HashMap<Partition, CSVWriter>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String key = schemaName + '_' + cubeName;
    badRecordslogger = new BadRecordslogger(key, "Partition_" + System.currentTimeMillis() + ".log",
        getBadLogStoreLocation("partition/" + schemaName + '/' + cubeName));

    CSVReader dataInputStream = null;

    long recordCounter = 0;

    CSVParser customParser = getCustomParser(delimiter, quoteChar, escapeChar);

    for (int i = 0; i < sourceFilePath.size(); i++) {
      try {
        CarbonFile file = FileFactory
            .getCarbonFile(sourceFilePath.get(i), FileFactory.getFileType(sourceFilePath.get(i)));
        // File file = new File(sourceFilePath);
        String fileAbsolutePath = file.getAbsolutePath();
        String fileName = null;
        if (!sourceFilesBasePath.endsWith(".csv") && fileAbsolutePath
            .startsWith(sourceFilesBasePath)) {
          if (sourceFilesBasePath.endsWith(File.separator)) {
            fileName = fileAbsolutePath.substring(sourceFilesBasePath.length())
                .replace(File.separator, "_");
          } else {
            fileName = fileAbsolutePath.substring(sourceFilesBasePath.length() + 1)
                .replace(File.separator, "_");
          }
        } else {
          fileName = file.getName();
        }

        // Read and prepare columns from first row in file
        DataInputStream inputStream = FileFactory.getDataInputStream(sourceFilePath.get(i),
            FileFactory.getFileType(sourceFilePath.get(i)));
        if (fileName.endsWith(".gz")) {
          GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
          dataInputStream =
              new CSVReader(new InputStreamReader(gzipInputStream, Charset.defaultCharset()),
                  CSVReader.DEFAULT_SKIP_LINES, customParser);
          fileName = fileName.substring(0, fileName.indexOf(".gz"));
        } else if (fileName.endsWith(".bz2")) {
          BZip2CompressorInputStream stream = new BZip2CompressorInputStream(inputStream);
          dataInputStream = new CSVReader(new InputStreamReader(stream, Charset.defaultCharset()),
              CSVReader.DEFAULT_SKIP_LINES, customParser);
          fileName = fileName.substring(0, fileName.indexOf(".bz2"));
        } else if (fileName.endsWith(".csv")) {
          dataInputStream =
              new CSVReader(new InputStreamReader(inputStream, Charset.defaultCharset()),
                  CSVReader.DEFAULT_SKIP_LINES, customParser);
          fileName = fileName.substring(0, fileName.indexOf(".csv"));
        } else {
          LOGGER.info("Processing file split: Unsupported File Extension: Skipping File : "
              + file.getAbsolutePath());
          partialSuccess = true;
          return;
        }
        dataInputStream.setBadRecordsLogger(badRecordslogger);
        if (fileHeader == null || fileHeader.length() == 0) {
          headerColumns = dataInputStream.readNext();
        } else {
          headerColumns = fileHeader.split(",");
        }
        if (null == headerColumns) {
          LOGGER.info("Csv file does not contain the header column neither the headers are "
                  + "passed in DDL or API. Skipping file :: " + sourceFilePath);
          partialSuccess = true;
          return;
        }
        int[] indexes = pruneColumnsAndGetIndexes(headerColumns, requiredColumns);

        // In case there is a dummy measure required columns length and
        // header columns length will not be equal
        if ((null == fileHeader || 0 == fileHeader.length()) && (0 == indexes.length) && (
            fileHeader.length() != indexes.length)) {
          LOGGER.info("Column headers are invalid. They do not match with the schema headers."
                  + "Skipping file :: " + sourceFilePath);
          partialSuccess = true;
          return;
        }

        partitionData(targetFolder, nodes, partitionCount, partitionColumn, headerColumns,
            outputStreamsMap, dataInputStream, recordCounter, fileName, indexes, fileAbsolutePath);
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
      } finally {
        CarbonUtil.closeStreams(dataInputStream);

        for (CSVWriter dataOutStream : outputStreamsMap.values()) {
          CarbonUtil.closeStreams(dataOutStream);
        }
        badRecordslogger.closeStreams();
      }
    }
  }

  private void partitionData(String targetFolder, List<String> nodes, int partitionCount,
      String[] partitionColumn, String[] headerColumns,
      HashMap<Partition, CSVWriter> outputStreamsMap, CSVReader dataInputStream, long recordCounter,
      String fileName, int[] indexes, String fileAbsolutePath)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
    DataPartitioner dataPartitioner =
        getDataPartitioner(targetFolder, nodes, partitionCount, partitionColumn, headerColumns);

    //Get partitions and create output streams
    List<Partition> allPartitions = dataPartitioner.getAllPartitions();

    loopPartitionsAndPopulateOutStreamMap(outputStreamsMap, fileName, allPartitions);

    //Write header in all the target files
    for (CSVWriter dataOutStream : outputStreamsMap.values()) {
      dataOutStream.writeNext(pruneColumns(headerColumns, indexes));
    }

    recordCounter = writeTargetStream(outputStreamsMap, dataInputStream, recordCounter, indexes,
        dataPartitioner, headerColumns, fileAbsolutePath);

    LOGGER
        .info("Processed Record count: " + recordCounter);
  }

  private CSVParser getCustomParser(String delimiter, String quoteChar, String escapeChar) {
    CSVParser customParser = null;
    boolean ignoreQuote = false;
    boolean ignoreEscape = false;
    char defaultQuoteChar = CSVParser.DEFAULT_QUOTE_CHARACTER;
    char defaultEscapeChar = CSVParser.DEFAULT_ESCAPE_CHARACTER;
    if (quoteChar == null || quoteChar.isEmpty() || quoteChar.trim().isEmpty()) {
      ignoreQuote = true;
    } else {
      ignoreQuote = false;
      defaultQuoteChar = quoteChar.charAt(0);
    }
    if (escapeChar == null || escapeChar.isEmpty() || escapeChar.trim().isEmpty()) {
      ignoreEscape = true;
    } else {
      ignoreEscape = false;
      defaultEscapeChar = escapeChar.charAt(0);
    }
    delimiter = CarbonUtil.unescapeChar(delimiter);
    customParser = new CSVParser(delimiter.charAt(0), defaultQuoteChar, defaultEscapeChar,
        CSVParser.DEFAULT_STRICT_QUOTES, CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE, ignoreQuote,
        ignoreEscape);
    return customParser;
  }

  private DataPartitioner getDataPartitioner(String targetFolder, List<String> nodes,
      int partitionCount, String[] partitionColumn, String[] headerColumns)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    DataPartitioner dataPartitioner =
        (DataPartitioner) Class.forName(partitionerClass).newInstance();

    Partitioner partitioner = new Partitioner(partitionerClass, partitionColumn, partitionCount,
        nodes.toArray(new String[nodes.size()]));
    //Initialise the partitioner
    dataPartitioner.initialize(targetFolder, headerColumns, partitioner);
    return dataPartitioner;
  }

  private long writeTargetStream(HashMap<Partition, CSVWriter> outputStreamsMap,
      CSVReader dataInputStream, long recordCounter, int[] indexes, DataPartitioner dataPartitioner,
      String[] headerColumns, String fileAbsolutePath) throws IOException {
    String[] record = null;
    Partition tartgetPartition = null;
    CSVWriter targetStream = null;
    record = dataInputStream.readNext();
    int skippedLines = 0;
    if (null == record) {
      return recordCounter;
    } else {
      boolean isEqual = compareHeaderColumnWithFirstRecordInCSV(headerColumns, record);
      if (isEqual) {
        record = dataInputStream.readNext();
        recordCounter++;
      }
    }
    while (null != record) {
      tartgetPartition = dataPartitioner.getPartionForTuple(record, recordCounter);
      targetStream = outputStreamsMap.get(tartgetPartition);
      try {
        targetStream.writeNext(pruneColumns(record, indexes));
      } catch (ArrayIndexOutOfBoundsException e) {
        partialSuccess = true;
        skippedLines++;
        badRecordslogger.addBadRecordsToBilder(record, record.length,
            "No. of columns not matched with cube columns", null);
        LOGGER.error("BAD Record Found: No. of columns not matched with cube columns, "
            + "Skipping line: (" + (recordCounter + 1) + ") in File :" + fileAbsolutePath);
      } catch (Exception e) {
        partialSuccess = true;
        skippedLines++;
        badRecordslogger.addBadRecordsToBilder(record, record.length, e.getMessage(), null);
        LOGGER.info("Exception while processing the record at line " + (recordCounter + 1)
            + " in partiton " + tartgetPartition.getUniqueID());
      } finally {
        record = dataInputStream.readNext();
        recordCounter++;
      }
    }
    if (skippedLines != 0) {
      LOGGER.info("No. of bad records skipped: (" + skippedLines + ") in file:" + fileAbsolutePath);
    }
    return recordCounter;
  }

  private boolean compareHeaderColumnWithFirstRecordInCSV(String[] headerColumns,
      String[] firstRecord) {
    String header = StringUtils.join(headerColumns, ',');
    String record = StringUtils.join(firstRecord, ',');
    if (header != null && header.equals(record)) {
      return true;
    }
    return false;
  }

  private void loopPartitionsAndPopulateOutStreamMap(HashMap<Partition, CSVWriter> outputStreamsMap,
      String fileName, List<Partition> allPartitions) throws IOException {
    for (Partition partition : allPartitions) {
      String targetFolderPath = partition.getFilePath();
      FileType fileType = FileFactory.getFileType(targetFolderPath);
      FileFactory.mkdirs(targetFolderPath, fileType);
      outputStreamsMap.put(partition, new CSVWriter(new OutputStreamWriter(FileFactory
          .getDataOutputStream(
              targetFolderPath + '/' + fileName + '_' + partition.getUniqueID() + ".csv", fileType,
              (short) 1), Charset.defaultCharset())));
    }
  }

  private int[] pruneColumnsAndGetIndexes(String[] headerColumns, String[] requiredColumns) {
    if (requiredColumns == null) {
      requiredColumns = headerColumns;
    }
    List<Integer> indexesList = new ArrayList<Integer>();
    for (int i = 0; i < headerColumns.length; i++) {
      for (int j = 0; j < requiredColumns.length; j++) {
        if (headerColumns[i].equalsIgnoreCase(requiredColumns[j])) {
          indexesList.add(i);
          break;
        }
      }
    }
    int[] indexes = new int[indexesList.size()];
    for (int i = 0; i < indexesList.size(); i++) {
      indexes[i] = indexesList.get(i);
    }
    return indexes;
  }

  private String[] pruneColumns(String[] tuple, int[] indexes) {
    String[] sb = new String[indexes.length];
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      sb[i] = tuple[indexes[i]];
    }
    return sb;
  }

  private String getBadLogStoreLocation(String storeLocation) {
    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    return badLogStoreLocation;
  }
}

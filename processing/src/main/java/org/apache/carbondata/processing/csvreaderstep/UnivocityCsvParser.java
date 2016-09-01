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
package org.apache.carbondata.processing.csvreaderstep;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * Class which will be used to read the data from csv file and parse the record
 */
public class UnivocityCsvParser {

  private LogService LOGGER = LogServiceFactory.getLogService(this.getClass().getName());

  /**
   * Max number of columns that will be parsed for a row by univocity parsing
   */
  private static final int DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING = 2000;
  /**
   * reader for csv
   */
  private Reader inputStreamReader;

  /**
   * buffer size of stream
   */
  private int bufferSize;

  /**
   * to keep track how many block has been processed
   */
  private int blockCounter = -1;

  /**
   * csv record parser which read and convert the record to csv format
   */
  private CsvParser parser;

  /**
   * row from csv
   */
  private String[] row;

  /**
   * holding all the properties required for parsing the records
   */
  private UnivocityCsvParserVo csvParserVo;

  public UnivocityCsvParser(UnivocityCsvParserVo csvParserVo) {
    this.csvParserVo = csvParserVo;
    bufferSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
            CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
  }

  /**
   * Below method will be used to initialize the the parser
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    CsvParserSettings parserSettings = new CsvParserSettings();
    parserSettings.getFormat().setDelimiter(csvParserVo.getDelimiter().charAt(0));
    parserSettings.getFormat().setComment(csvParserVo.getCommentCharacter().charAt(0));
    parserSettings.setLineSeparatorDetectionEnabled(true);
    parserSettings.setMaxColumns(
        getMaxColumnsForParsing(csvParserVo.getNumberOfColumns(), csvParserVo.getMaxColumns()));
    parserSettings.setNullValue("");
    parserSettings.setIgnoreLeadingWhitespaces(false);
    parserSettings.setIgnoreTrailingWhitespaces(false);
    parserSettings.setSkipEmptyLines(false);
    parserSettings.getFormat().setQuote(null == csvParserVo.getQuoteCharacter() ?
        '\"':csvParserVo.getQuoteCharacter().charAt(0));
    parserSettings.getFormat().setQuoteEscape(null == csvParserVo.getEscapeCharacter() ?
        '\\' :
        csvParserVo.getEscapeCharacter().charAt(0));
    blockCounter++;
    initializeReader();
    if (csvParserVo.getBlockDetailsList().get(blockCounter).getBlockOffset() == 0) {
      parserSettings.setHeaderExtractionEnabled(csvParserVo.isHeaderPresent());
    }
    parser = new CsvParser(parserSettings);
    parser.beginParsing(inputStreamReader);
  }

  /**
   * This method will decide the number of columns to be parsed for a row by univocity parser
   *
   * @param columnCountInSchema total number of columns in schema
   * @return
   */
  private int getMaxColumnsForParsing(int columnCountInSchema, int maxColumns) {
    int maxNumberOfColumnsForParsing = DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING;
    if (maxColumns > 0) {
      if (columnCountInSchema > maxColumns) {
        maxNumberOfColumnsForParsing = columnCountInSchema + 10;
      } else {
        maxNumberOfColumnsForParsing = maxColumns;
      }
    } else if (columnCountInSchema > DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
      maxNumberOfColumnsForParsing = columnCountInSchema + 10;
    }
    return maxNumberOfColumnsForParsing;
  }

  /**
   * Below method will be used to initialize the reader
   *
   * @throws IOException
   */
  private void initializeReader() throws IOException {
    // if already one input stream is open first we need to close and then
    // open new stream
    close();

    String path = this.csvParserVo.getBlockDetailsList().get(blockCounter).getFilePath();
    FileType fileType = FileFactory.getFileType(path);

    if (path.endsWith(".gz")) {
      DataInputStream dataInputStream = FileFactory.getDataInputStream(path, fileType, bufferSize);
      inputStreamReader = new BufferedReader(new InputStreamReader(dataInputStream));
    } else {
      long startOffset = this.csvParserVo.getBlockDetailsList().get(blockCounter).getBlockOffset();
      long blockLength = this.csvParserVo.getBlockDetailsList().get(blockCounter).getBlockLength();
      long endOffset = blockLength + startOffset;

      DataInputStream dataInputStream =
          FileFactory.getDataInputStream(path, fileType, bufferSize, startOffset);
      // if start offset is not 0 then reading then reading and ignoring the extra line
      if (startOffset != 0) {
        LineReader lineReader = new LineReader(dataInputStream, 1);
        startOffset += lineReader.readLine(new Text(), 0);
      }
      inputStreamReader = new BufferedReader(new InputStreamReader(
          new BoundedDataStream(dataInputStream, endOffset - startOffset)));
    }
  }

  /**
   * Below method will be used to clear all the stream
   */
  public void close() {
    if (null != inputStreamReader) {
      CarbonUtil.closeStreams(inputStreamReader);
    }

  }

  /**
   * Below method will be used to check whether any more records is present or
   * not
   *
   * @return true if more records are present
   * @throws IOException
   */
  public boolean hasMoreRecords() throws IOException {
    row = parser.parseNext();
    if (row == null && blockCounter + 1 >= this.csvParserVo.getBlockDetailsList().size()) {
      close();
      return false;
    }
    if (row == null) {
      initialize();
      row = parser.parseNext();
    }
    return true;
  }

  /**
   * Below method will be used to get the new record
   *
   * @return next record
   */
  public String[] getNextRecord() {
    String[] returnValue = row;
    row = null;
    return returnValue;
  }
}

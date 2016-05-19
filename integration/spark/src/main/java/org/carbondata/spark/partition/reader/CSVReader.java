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

package org.carbondata.spark.partition.reader;

/**
 * Copyright 2005 Bytecode Pty Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordslogger;

/**
 * A very simple CSV reader released under a commercial-friendly license.
 *
 * @author Glen Smith
 */
public class CSVReader implements Closeable, Iterable<String[]> {

  public static final boolean DEFAULT_KEEP_CR = false;
  public static final boolean DEFAULT_VERIFY_READER = true;
  /**
   * The default line to start reading.
   */
  public static final int DEFAULT_SKIP_LINES = 0;
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CSVReader.class.getName());
  private CSVParser parser;
  private int skipLines;
  private BufferedReader br;
  private LineReader lineReader;
  private boolean hasNext = true;
  private boolean linesSkiped;
  private boolean keepCR;
  private boolean verifyReader;
  private long lineNum;
  private long skippedLines;
  private boolean multiLine = true;
  private BadRecordslogger badRecordslogger;

  /**
   * Constructs CSVReader using a comma for the separator.
   *
   * @param reader the reader to an underlying CSV source.
   */
  public CSVReader(Reader reader) {
    this(reader, CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER,
        CSVParser.DEFAULT_ESCAPE_CHARACTER);
  }

  /**
   * Constructs CSVReader with supplied separator.
   *
   * @param reader    the reader to an underlying CSV source.
   * @param separator the delimiter to use for separating entries.
   */
  public CSVReader(Reader reader, char separator) {
    this(reader, separator, CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER);
  }

  /**
   * Constructs CSVReader with supplied separator and quote char.
   *
   * @param reader    the reader to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   */
  public CSVReader(Reader reader, char separator, char quotechar) {
    this(reader, separator, quotechar, CSVParser.DEFAULT_ESCAPE_CHARACTER, DEFAULT_SKIP_LINES,
        CSVParser.DEFAULT_STRICT_QUOTES);
  }

  /**
   * Constructs CSVReader with supplied separator, quote char and quote handling
   * behavior.
   *
   * @param reader       the reader to an underlying CSV source.
   * @param separator    the delimiter to use for separating entries
   * @param quotechar    the character to use for quoted elements
   * @param strictQuotes sets if characters outside the quotes are ignored
   */
  public CSVReader(Reader reader, char separator, char quotechar, boolean strictQuotes) {
    this(reader, separator, quotechar, CSVParser.DEFAULT_ESCAPE_CHARACTER, DEFAULT_SKIP_LINES,
        strictQuotes);
  }

  /**
   * Constructs CSVReader.
   *
   * @param reader    the reader to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   * @param escape    the character to use for escaping a separator or quote
   */

  public CSVReader(Reader reader, char separator, char quotechar, char escape) {
    this(reader, separator, quotechar, escape, DEFAULT_SKIP_LINES, CSVParser.DEFAULT_STRICT_QUOTES);
  }

  /**
   * Constructs CSVReader.
   *
   * @param reader    the reader to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   * @param line      the line number to skip for start reading
   */
  public CSVReader(Reader reader, char separator, char quotechar, int line) {
    this(reader, separator, quotechar, CSVParser.DEFAULT_ESCAPE_CHARACTER, line,
        CSVParser.DEFAULT_STRICT_QUOTES);
  }

  /**
   * Constructs CSVReader.
   *
   * @param reader    the reader to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   * @param escape    the character to use for escaping a separator or quote
   * @param line      the line number to skip for start reading
   */
  public CSVReader(Reader reader, char separator, char quotechar, char escape, int line) {
    this(reader, separator, quotechar, escape, line, CSVParser.DEFAULT_STRICT_QUOTES);
  }

  /**
   * Constructs CSVReader.
   *
   * @param reader       the reader to an underlying CSV source.
   * @param separator    the delimiter to use for separating entries
   * @param quotechar    the character to use for quoted elements
   * @param escape       the character to use for escaping a separator or quote
   * @param line         the line number to skip for start reading
   * @param strictQuotes sets if characters outside the quotes are ignored
   */
  public CSVReader(Reader reader, char separator, char quotechar, char escape, int line,
      boolean strictQuotes) {
    this(reader, separator, quotechar, escape, line, strictQuotes,
        CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
  }

  /**
   * Constructs CSVReader with all data entered.
   *
   * @param reader                  the reader to an underlying CSV source.
   * @param separator               the delimiter to use for separating entries
   * @param quotechar               the character to use for quoted elements
   * @param escape                  the character to use for escaping a separator or quote
   * @param line                    the line number to skip for start reading
   * @param strictQuotes            sets if characters outside the quotes are ignored
   * @param ignoreLeadingWhiteSpace it true, parser should ignore white space before a quote
   *                                in a field
   */
  public CSVReader(Reader reader, char separator, char quotechar, char escape, int line,
      boolean strictQuotes, boolean ignoreLeadingWhiteSpace) {
    this(reader, line,
        new CSVParser(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace));
  }

  /**
   * Constructs CSVReader with all data entered.
   *
   * @param reader                  the reader to an underlying CSV source.
   * @param separator               the delimiter to use for separating entries
   * @param quotechar               the character to use for quoted elements
   * @param escape                  the character to use for escaping a separator or quote
   * @param line                    the line number to skip for start reading
   * @param strictQuotes            sets if characters outside the quotes are ignored
   * @param ignoreLeadingWhiteSpace if true, parser should ignore white space before a quote
   *                                in a field
   * @param keepCR                  if true the reader will keep carriage returns,
   *                                otherwise it will discard them.
   */
  public CSVReader(Reader reader, char separator, char quotechar, char escape, int line,
      boolean strictQuotes, boolean ignoreLeadingWhiteSpace, boolean keepCR) {
    this(reader, line,
        new CSVParser(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace), keepCR,
        DEFAULT_VERIFY_READER);
  }

  /**
   * Constructs CSVReader with supplied CSVParser.
   *
   * @param reader    the reader to an underlying CSV source.
   * @param line      the line number to skip for start reading
   * @param csvParser the parser to use to parse input
   */
  public CSVReader(Reader reader, int line, CSVParser csvParser, boolean multiLine) {
    this(reader, line, csvParser);
    this.multiLine = multiLine;
  }

  public CSVReader(Reader reader, int line, CSVParser csvParser) {
    this(reader, line, csvParser, DEFAULT_KEEP_CR, DEFAULT_VERIFY_READER);
  }

  /**
   * Constructs CSVReader with supplied CSVParser.
   *
   * @param reader       the reader to an underlying CSV source.
   * @param line         the line number to skip for start reading
   * @param csvParser    the parser to use to parse input
   * @param keepCR       true to keep carriage returns in data read, false otherwise
   * @param verifyReader true to verify reader before each read, false otherwise
   */
  CSVReader(Reader reader, int line, CSVParser csvParser, boolean keepCR, boolean verifyReader) {
    this.br = (reader instanceof BufferedReader ?
        (BufferedReader) reader :
        new BufferedReader(reader, 30720));
    this.lineReader = new LineReader(br, keepCR);
    this.skipLines = line;
    this.parser = csvParser;
    this.keepCR = keepCR;
    this.verifyReader = verifyReader;
  }

  /**
   * @return the CSVParser used by the reader.
   */
  public CSVParser getParser() {
    return parser;
  }

  /**
   * Returns the number of lines in the csv file to skip before processing.  This is
   * useful when there is miscellaneous data at the beginning of a file.
   *
   * @return the number of lines in the csv file to skip before processing.
   */
  public int getSkipLines() {
    return skipLines;
  }

  /**
   * Returns if the reader will keep carriage returns found in data or remove them.
   *
   * @return true if reader will keep carriage returns, false otherwise.
   */
  public boolean keepCarriageReturns() {
    return keepCR;
  }

  /**
   * Reads the entire file into a List with each element being a String[] of
   * tokens.
   *
   * @return a List of String[], with each String[] representing a line of the
   * file.
   * @throws IOException if bad things happen during the read
   */
  public List<String[]> readAll() throws IOException {

    //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
    List<String[]> allElements = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    while (hasNext) {
      String[] nextLineAsTokens = readNext();
      if (nextLineAsTokens != null) {
        allElements.add(nextLineAsTokens);
      }
    }
    return allElements;
    //CHECKSTYLE:ON
  }

  /**
   * Reads the next line from the buffer and converts to a string array.
   *
   * @return a string array with each comma-separated element as a separate
   * entry.
   * @throws IOException if bad things happen during the read
   */
  public String[] readNext() throws IOException {

    if (this.multiLine) {
      return readMultiLine();
    } else {
      return readSingleLine();
    }
  }

  private String[] readSingleLine() throws IOException {
    String[] result = null;
    String nextLine = getNextLine();
    try {
      this.lineNum += 1L;
      result = parser.parseLine(nextLine);
    } catch (IOException e) {
      if ("Un-terminated quoted field at end of CSV line".equals(e.getMessage())) {
        badRecordslogger.addBadRecordsToBilder(new String[] { nextLine }, 1,
            "Un-terminated quoted field at end of CSV line", null);
        LOGGER.info("Found Un-terminated quote @ line [" + this.lineNum + "] : Skipping Line : "
                + nextLine);
        this.skippedLines += 1L;
        result = readNext();
      } else {
        throw e;
      }
    }
    if (null == nextLine) {
      LOGGER.info("Total Number of Lines : " + --this.lineNum);
      LOGGER.info("Number of Lines Skipped: " + this.skippedLines);
      //            System.out.println("Total Number of Lines : "+ --this.lineNum);
      //            System.out.println("Number of Lines Skipped: "+ this.skippedLines);
    }
    return result;
  }

  private String[] readMultiLine() throws IOException {
    int linesread = 0;
    String[] result = null;
    String firstLine = null;
    do {
      this.lineNum += 1L;
      linesread++;
      if (linesread == 2) {
        br.mark(12000);
      }
      String nextLine = getNextLine();
      if (!hasNext) {
        LOGGER.info("Total Number of Lines : " + --this.lineNum);
        LOGGER.info("Number of Lines Skipped: " + this.skippedLines);
        //             System.out.println("Total Number of Lines : "+ --this.lineNum);
        //             System.out.println("Number of Lines Skipped: "+ this.skippedLines);
        return result; // should throw if still pending?
      }
      try {
        String[] r = parser.parseLineMulti(nextLine);
        if (r.length > 0) {
          if (result == null) {
            result = r;
          } else {
            result = combineResultsFromMultipleReads(result, r);
          }
        }
      } catch (IOException e) {
        if ("Un-terminated quoted field after 10000 characters".equals(e.getMessage())) {
          LOGGER.info("Un-terminated quoted field found after 10000 characters in MultiLine "
                  + "(No. Of Line searched : " + linesread + " ) starting from Line :" + (
                  this.lineNum - linesread + 1));
          LOGGER.info("Skipped Line Info : " + firstLine);
          parser.setPending(null);
          this.skippedLines += 1;
          this.lineNum += (1 - linesread);
          if (linesread > 1) {
            br.reset();
          }
          int resLength = result != null ? result.length : 0;
          badRecordslogger.addBadRecordsToBilder(result, resLength,
              "Un-terminated quoted field after 10000 characters", null);
          result = readNext();
        } else {
          throw e;
        }
      }
      if (linesread == 1) {
        firstLine = nextLine;
      }
      //            String[] r = parser.parseLine(nextLine);
    } while (parser.isPending());
    return result;
  }

  /**
   * For multi line records this method combines the current result with the result
   * from previous read(s).
   *
   * @param buffer   - previous data read for this record
   * @param lastRead - latest data read for this record.
   * @return String array with union of the buffer and lastRead arrays.
   */
  private String[] combineResultsFromMultipleReads(String[] buffer, String[] lastRead) {
    String[] t = new String[buffer.length + lastRead.length];
    System.arraycopy(buffer, 0, t, 0, buffer.length);
    System.arraycopy(lastRead, 0, t, buffer.length, lastRead.length);
    return t;
  }

  /**
   * Reads the next line from the file.
   *
   * @return the next line from the file without trailing newline
   * @throws IOException if bad things happen during the read
   */
  public String getNextLine() throws IOException {
    if (isClosed()) {
      hasNext = false;
      return null;
    }

    if (!this.linesSkiped) {
      for (int i = 0; i < skipLines; i++) {
        lineReader.readLine();
      }
      this.linesSkiped = true;
    }
    String nextLine = lineReader.readLine();
    if (nextLine == null) {
      hasNext = false;
    }
    return hasNext ? nextLine : null;
  }

  /**
   * Checks to see if the file is closed.
   *
   * @return true if the reader can no longer be read from.
   */
  private boolean isClosed() {
    if (!verifyReader) {
      return false;
    }
    try {
      return !br.ready();
    } catch (IOException e) {
      return true;
    }
  }

  /**
   * Closes the underlying reader.
   *
   * @throws IOException if the close fails
   */
  public void close() throws IOException {
    br.close();
  }

  /**
   * Creates an Iterator for processing the csv data.
   *
   * @return an String[] iterator.
   */
  public Iterator<String[]> iterator() {
    try {
      return new CSVIterator(this);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns if the CSVReader will verify the reader before each read.
   * By default the value is true which is the functionality for version 3.0.
   * If set to false the reader is always assumed ready to read - this is the functionality
   * for version 2.4 and before.
   * The reason this method was needed was that certain types of Readers would return
   * false for its ready() method until a read was done (namely readers created using Channels).
   * This caused opencsv not to read from those readers.
   *
   * @return true if CSVReader will verify the reader before reads.  False otherwise.
   * @link https://sourceforge.net/p/opencsv/bugs/108/
   */
  public boolean verifyReader() {
    return this.verifyReader;
  }

  public void setBadRecordsLogger(BadRecordslogger badRecordslogger) {
    this.badRecordslogger = badRecordslogger;
  }
}

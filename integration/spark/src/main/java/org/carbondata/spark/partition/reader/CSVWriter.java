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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;

/**
 * A very simple CSV writer released under a commercial-friendly license.
 *
 * @author Glen Smith
 */
public class CSVWriter implements Closeable, Flushable {

  public static final int INITIAL_STRING_SIZE = 128;
  /**
   * The character used for escaping quotes.
   */
  public static final char DEFAULT_ESCAPE_CHARACTER = '"';
  /**
   * The default separator to use if none is supplied to the constructor.
   */
  public static final char DEFAULT_SEPARATOR = ',';
  /**
   * The default quote character to use if none is supplied to the
   * constructor.
   */
  public static final char DEFAULT_QUOTE_CHARACTER = '"';
  /**
   * The quote constant to use when you wish to suppress all quoting.
   */
  public static final char NO_QUOTE_CHARACTER = '\u0000';
  /**
   * The escape constant to use when you wish to suppress all escaping.
   */
  public static final char NO_ESCAPE_CHARACTER = '\u0000';
  /**
   * Default line terminator uses platform encoding.
   */
  public static final String DEFAULT_LINE_END = "\n";

  public static final String CARRIAGE_RETURN = "\r";

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CSVWriter.class.getName());

  private Writer rawWriter;
  private PrintWriter pw;
  private char separator;
  private char quotechar;
  private char escapechar;
  private String lineEnd;
  private ResultSetHelper resultService = new ResultSetHelperService();

  /**
   * Constructs CSVWriter using a comma for the separator.
   *
   * @param writer the writer to an underlying CSV source.
   */
  public CSVWriter(Writer writer) {
    this(writer, DEFAULT_SEPARATOR);
  }

  /**
   * Constructs CSVWriter with supplied separator.
   *
   * @param writer    the writer to an underlying CSV source.
   * @param separator the delimiter to use for separating entries.
   */
  public CSVWriter(Writer writer, char separator) {
    this(writer, separator, DEFAULT_QUOTE_CHARACTER);
  }

  /**
   * Constructs CSVWriter with supplied separator and quote char.
   *
   * @param writer    the writer to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   */
  public CSVWriter(Writer writer, char separator, char quotechar) {
    this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
  }

  /**
   * Constructs CSVWriter with supplied separator and quote char.
   *
   * @param writer     the writer to an underlying CSV source.
   * @param separator  the delimiter to use for separating entries
   * @param quotechar  the character to use for quoted elements
   * @param escapechar the character to use for escaping quotechars or escapechars
   */
  public CSVWriter(Writer writer, char separator, char quotechar, char escapechar) {
    this(writer, separator, quotechar, escapechar, DEFAULT_LINE_END);
  }

  /**
   * Constructs CSVWriter with supplied separator and quote char.
   *
   * @param writer    the writer to an underlying CSV source.
   * @param separator the delimiter to use for separating entries
   * @param quotechar the character to use for quoted elements
   * @param lineEnd   the line feed terminator to use
   */
  public CSVWriter(Writer writer, char separator, char quotechar, String lineEnd) {
    this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER, lineEnd);
  }

  /**
   * Constructs CSVWriter with supplied separator, quote char, escape char and line ending.
   *
   * @param writer     the writer to an underlying CSV source.
   * @param separator  the delimiter to use for separating entries
   * @param quotechar  the character to use for quoted elements
   * @param escapechar the character to use for escaping quotechars or escapechars
   * @param lineEnd    the line feed terminator to use
   */
  public CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd) {
    this.rawWriter = writer;
    this.pw = new PrintWriter(writer);
    this.separator = separator;
    this.quotechar = quotechar;
    this.escapechar = escapechar;
    this.lineEnd = lineEnd;
  }

  /**
   * Writes the entire list to a CSV file. The list is assumed to be a
   * String[]
   *
   * @param allLines         a List of String[], with each String[] representing a line of
   *                         the file.
   * @param applyQuotesToAll true if all values are to be quoted.  false if quotes only
   *                         to be applied to values which contain the separator, escape,
   *                         quote or new line characters.
   */
  public void writeAll(List<String[]> allLines, boolean applyQuotesToAll) {
    for (String[] line : allLines) {
      writeNext(line, applyQuotesToAll);
    }
  }

  /**
   * Writes the entire list to a CSV file. The list is assumed to be a
   * String[]
   *
   * @param allLines a List of String[], with each String[] representing a line of
   *                 the file.
   */
  public void writeAll(List<String[]> allLines) {
    for (String[] line : allLines) {
      writeNext(line);
    }
  }

  /**
   * Writes the column names.
   *
   * @param rs - ResultSet containing column names.
   * @throws SQLException - thrown by ResultSet::getColumnNames
   */
  protected void writeColumnNames(ResultSet rs) throws SQLException {

    writeNext(resultService.getColumnNames(rs));
  }

  /**
   * Writes the entire ResultSet to a CSV file.
   * The caller is responsible for closing the ResultSet.
   *
   * @param rs                 the result set to write
   * @param includeColumnNames true if you want column names in the output, false otherwise
   * @throws IOException  thrown by getColumnValue
   * @throws SQLException thrown by getColumnValue
   */
  public void writeAll(ResultSet rs, boolean includeColumnNames) throws SQLException, IOException {
    writeAll(rs, includeColumnNames, false);
  }

  /**
   * Writes the entire ResultSet to a CSV file.
   * The caller is responsible for closing the ResultSet.
   *
   * @param rs                 the Result set to write.
   * @param includeColumnNames include the column names in the output.
   * @param trim               remove spaces from the data before writing.
   * @throws IOException  thrown by getColumnValue
   * @throws SQLException thrown by getColumnValue
   */
  public void writeAll(ResultSet rs, boolean includeColumnNames, boolean trim)
      throws SQLException, IOException {

    if (includeColumnNames) {
      writeColumnNames(rs);
    }

    while (rs.next()) {
      writeNext(resultService.getColumnValues(rs, trim));
    }
  }

  /**
   * Writes the next line to the file.
   *
   * @param nextLine         a string array with each comma-separated element as a separate
   *                         entry.
   * @param applyQuotesToAll true if all values are to be quoted.  false applies quotes only
   *                         to values which contain the separator, escape, quote or new line
   *                         characters.
   */
  public void writeNext(String[] nextLine, boolean applyQuotesToAll) {

    if (nextLine == null) {
      return;
    }

    StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
    for (int i = 0; i < nextLine.length; i++) {

      if (i != 0) {
        sb.append(separator);
      }

      String nextElement = nextLine[i];

      if (nextElement == null) {
        continue;
      }

      Boolean stringContainsSpecialCharacters = stringContainsSpecialCharacters(nextElement);

      if ((applyQuotesToAll || stringContainsSpecialCharacters)
          && quotechar != NO_QUOTE_CHARACTER) {
        sb.append(quotechar);
      }

      if (stringContainsSpecialCharacters) {
        sb.append(processLine(nextElement));
      } else {
        sb.append(nextElement);
      }

      if ((applyQuotesToAll || stringContainsSpecialCharacters)
          && quotechar != NO_QUOTE_CHARACTER) {
        sb.append(quotechar);
      }
    }

    sb.append(lineEnd);
    pw.write(sb.toString());
  }

  /**
   * Writes the next line to the file.
   *
   * @param nextLine a string array with each comma-separated element as a separate
   *                 entry.
   */
  public void writeNext(String[] nextLine) {
    writeNext(nextLine, true);
  }

  /**
   * checks to see if the line contains special characters.
   *
   * @param line - element of data to check for special characters.
   * @return true if the line contains the quote, escape, separator, newline or return.
   */
  private boolean stringContainsSpecialCharacters(String line) {
    return line.indexOf(quotechar) != -1 || line.indexOf(escapechar) != -1
        || line.indexOf(separator) != -1 || line.contains(DEFAULT_LINE_END) || line
        .contains(CARRIAGE_RETURN);
  }

  /**
   * Processes all the characters in a line.
   *
   * @param nextElement - element to process.
   * @return a StringBuilder with the elements data.
   */
  protected StringBuilder processLine(String nextElement) {
    StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
    for (int j = 0; j < nextElement.length(); j++) {
      char nextChar = nextElement.charAt(j);
      processCharacter(sb, nextChar);
    }

    return sb;
  }

  /**
   * Appends the character to the StringBuilder adding the escape character if needed.
   *
   * @param sb       - StringBuffer holding the processed character.
   * @param nextChar - character to process
   */
  private void processCharacter(StringBuilder sb, char nextChar) {
    if (escapechar != NO_ESCAPE_CHARACTER && (nextChar == quotechar || nextChar == escapechar)) {
      sb.append(escapechar).append(nextChar);
    } else {
      sb.append(nextChar);
    }
  }

  /**
   * Flush underlying stream to writer.
   *
   * @throws IOException if bad things happen
   */
  public void flush() throws IOException {

    pw.flush();

  }

  /**
   * Close the underlying stream writer flushing any buffered content.
   *
   * @throws IOException if bad things happen
   */
  public void close() throws IOException {
    flush();
    pw.close();
    rawWriter.close();
  }

  /**
   * Checks to see if the there has been an error in the printstream.
   *
   * @return <code>true</code> if the print stream has encountered an error,
   * either on the underlying output stream or during a format
   * conversion.
   */
  public boolean checkError() {
    return pw.checkError();
  }

  /**
   * Sets the result service.
   *
   * @param resultService - the ResultSetHelper
   */
  public void setResultService(ResultSetHelper resultService) {
    this.resultService = resultService;
  }

  /**
   * flushes the writer without throwing any exceptions.
   */
  public void flushQuietly() {
    try {
      flush();
    } catch (IOException e) {
      LOGGER.debug("Error while flushing");
    }
  }
}

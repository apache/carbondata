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
package org.apache.carbondata.hadoop.util;

import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.conf.Configuration;

/**
 * CSVInputFormatUtil is a util class.
 */
public class CSVInputFormatUtil {

  public static final String DELIMITER = "carbon.csvinputformat.delimiter";
  public static final String DELIMITER_DEFAULT = ",";
  public static final String COMMENT = "carbon.csvinputformat.comment";
  public static final String COMMENT_DEFAULT = "#";
  public static final String QUOTE = "carbon.csvinputformat.quote";
  public static final String QUOTE_DEFAULT = "\"";
  public static final String ESCAPE = "carbon.csvinputformat.escape";
  public static final String ESCAPE_DEFAULT = "\\";
  public static final String HEADER_PRESENT = "caron.csvinputformat.header.present";
  public static final boolean HEADER_PRESENT_DEFAULT = false;

  public static CsvParserSettings extractCsvParserSettings(Configuration job, long start) {
    CsvParserSettings parserSettings = new CsvParserSettings();
    parserSettings.getFormat().setDelimiter(job.get(DELIMITER, DELIMITER_DEFAULT).charAt(0));
    parserSettings.getFormat().setComment(job.get(COMMENT, COMMENT_DEFAULT).charAt(0));
    parserSettings.setLineSeparatorDetectionEnabled(true);
    parserSettings.setNullValue("");
    parserSettings.setIgnoreLeadingWhitespaces(false);
    parserSettings.setIgnoreTrailingWhitespaces(false);
    parserSettings.setSkipEmptyLines(false);
    parserSettings.getFormat().setQuote(job.get(QUOTE, QUOTE_DEFAULT).charAt(0));
    parserSettings.getFormat().setQuoteEscape(job.get(ESCAPE, ESCAPE_DEFAULT).charAt(0));
    if (start == 0) {
      parserSettings.setHeaderExtractionEnabled(job.getBoolean(HEADER_PRESENT,
          HEADER_PRESENT_DEFAULT));
    }
    return parserSettings;
  }
}

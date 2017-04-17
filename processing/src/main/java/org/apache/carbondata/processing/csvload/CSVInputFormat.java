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
package org.apache.carbondata.processing.csvload;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for csv files.  Files are broken into lines.
 * Values are the line of csv files.
 */
public class CSVInputFormat extends FileInputFormat<NullWritable, StringArrayWritable> {

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
  public static final String READ_BUFFER_SIZE = "carbon.csvinputformat.read.buffer.size";
  public static final String READ_BUFFER_SIZE_DEFAULT = "65536";
  public static final String MAX_COLUMNS = "carbon.csvinputformat.max.columns";
  public static final String NUMBER_OF_COLUMNS = "carbon.csvinputformat.number.of.columns";
  public static final int DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING = 2000;
  public static final int THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING = 20000;

  private static LogService LOGGER =
      LogServiceFactory.getLogService(CSVInputFormat.class.toString());


  @Override
  public RecordReader<NullWritable, StringArrayWritable> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new CSVRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration())
        .getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * Sets the comment char to configuration. Default it is #.
   * @param configuration
   * @param commentChar
   */
  public static void setCommentCharacter(Configuration configuration, String commentChar) {
    if (commentChar != null && !commentChar.isEmpty()) {
      configuration.set(COMMENT, commentChar);
    }
  }

  /**
   * Sets the delimiter to configuration. Default it is ','
   * @param configuration
   * @param delimiter
   */
  public static void setCSVDelimiter(Configuration configuration, String delimiter) {
    if (delimiter != null && !delimiter.isEmpty()) {
      configuration.set(DELIMITER, delimiter);
    }
  }

  /**
   * Sets the escape character to configuration. Default it is \
   * @param configuration
   * @param escapeCharacter
   */
  public static void setEscapeCharacter(Configuration configuration, String escapeCharacter) {
    if (escapeCharacter != null && !escapeCharacter.isEmpty()) {
      configuration.set(ESCAPE, escapeCharacter);
    }
  }

  /**
   * Whether header needs to read from csv or not. By default it is false.
   * @param configuration
   * @param headerExtractEnable
   */
  public static void setHeaderExtractionEnabled(Configuration configuration,
      boolean headerExtractEnable) {
    configuration.set(HEADER_PRESENT, String.valueOf(headerExtractEnable));
  }

  /**
   * Sets the quote character to configuration. Default it is "
   * @param configuration
   * @param quoteCharacter
   */
  public static void setQuoteCharacter(Configuration configuration, String quoteCharacter) {
    if (quoteCharacter != null && !quoteCharacter.isEmpty()) {
      configuration.set(QUOTE, quoteCharacter);
    }
  }

  /**
   * Sets the read buffer size to configuration.
   * @param configuration
   * @param bufferSize
   */
  public static void setReadBufferSize(Configuration configuration, String bufferSize) {
    if (bufferSize != null && !bufferSize.isEmpty()) {
      configuration.set(READ_BUFFER_SIZE, bufferSize);
    }
  }

  public static void setMaxColumns(Configuration configuration, String maxColumns) {
    if (maxColumns != null) {
      configuration.set(MAX_COLUMNS, maxColumns);
    }
  }

  public static void setNumberOfColumns(Configuration configuration, String numberOfColumns) {
    configuration.set(NUMBER_OF_COLUMNS, numberOfColumns);
  }

  /**
   * Treats value as line in file. Key is null.
   */
  public static class CSVRecordReader extends RecordReader<NullWritable, StringArrayWritable> {

    private long start;
    private long end;
    private BoundedInputStream boundedInputStream;
    private Reader reader;
    private CsvParser csvParser;
    private StringArrayWritable value;
    private String[] columns;
    private Seekable filePosition;
    private boolean isCompressedInput;
    private Decompressor decompressor;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      FileSplit split = (FileSplit) inputSplit;
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      Configuration job = context.getConfiguration();
      CompressionCodec codec = (new CompressionCodecFactory(job)).getCodec(file);
      FileSystem fs = file.getFileSystem(job);
      int bufferSize = Integer.parseInt(job.get(READ_BUFFER_SIZE, READ_BUFFER_SIZE_DEFAULT));
      FSDataInputStream fileIn = fs.open(file, bufferSize);
      InputStream inputStream;
      if (codec != null) {
        isCompressedInput = true;
        decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
          SplitCompressionInputStream scIn = ((SplittableCompressionCodec) codec)
              .createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec
                  .READ_MODE.BYBLOCK);
          start = scIn.getAdjustedStart();
          end = scIn.getAdjustedEnd();
          if (start != 0) {
            LineReader lineReader = new LineReader(scIn, 1);
            start += lineReader.readLine(new Text(), 0);
          }
          filePosition = scIn;
          inputStream = scIn;
        } else {
          CompressionInputStream cIn = codec.createInputStream(fileIn, decompressor);
          filePosition = cIn;
          inputStream = cIn;
        }
      } else {
        fileIn.seek(start);
        if (start != 0) {
          LineReader lineReader = new LineReader(fileIn, 1);
          start += lineReader.readLine(new Text(), 0);
        }
        boundedInputStream = new BoundedInputStream(fileIn, end - start);
        filePosition = fileIn;
        inputStream = boundedInputStream;
      }
      reader = new InputStreamReader(inputStream);
      csvParser = new CsvParser(extractCsvParserSettings(job));
      csvParser.beginParsing(reader);
    }

    private CsvParserSettings extractCsvParserSettings(Configuration job) {
      CsvParserSettings parserSettings = new CsvParserSettings();
      parserSettings.getFormat().setDelimiter(job.get(DELIMITER, DELIMITER_DEFAULT).charAt(0));
      parserSettings.getFormat().setComment(job.get(COMMENT, COMMENT_DEFAULT).charAt(0));
      parserSettings.setLineSeparatorDetectionEnabled(true);
      parserSettings.setNullValue("");
      parserSettings.setEmptyValue("");
      parserSettings.setIgnoreLeadingWhitespaces(false);
      parserSettings.setIgnoreTrailingWhitespaces(false);
      parserSettings.setSkipEmptyLines(false);
      parserSettings.setMaxCharsPerColumn(100000);
      String maxColumns = job.get(MAX_COLUMNS);
      parserSettings.setMaxColumns(Integer.parseInt(maxColumns));
      parserSettings.getFormat().setQuote(job.get(QUOTE, QUOTE_DEFAULT).charAt(0));
      parserSettings.getFormat().setQuoteEscape(job.get(ESCAPE, ESCAPE_DEFAULT).charAt(0));
      if (start == 0) {
        parserSettings.setHeaderExtractionEnabled(job.getBoolean(HEADER_PRESENT,
            HEADER_PRESENT_DEFAULT));
      }
      return parserSettings;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (csvParser == null) {
        return false;
      }
      columns = csvParser.parseNext();
      if (columns == null) {
        value = null;
        return false;
      }
      if (value == null) {
        value = new StringArrayWritable();
      }
      value.set(columns);
      return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public StringArrayWritable getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    private long getPos() throws IOException {
      long retVal = start;
      if (null != boundedInputStream) {
        retVal = end - boundedInputStream.getRemaining();
      } else if (isCompressedInput && null != filePosition) {
        retVal = filePosition.getPos();
      }
      return retVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return start == end ? 0.0F : Math.min(1.0F, (float) (getPos() -
          start) / (float) (end - start));
    }

    @Override
    public void close() throws IOException {
      try {
        if (reader != null) {
          reader.close();
        }
        if (boundedInputStream != null) {
          boundedInputStream.close();
        }
      } finally {
        reader = null;
        boundedInputStream = null;
        csvParser = null;
        filePosition = null;
        value = null;
        if (decompressor != null) {
          CodecPool.returnDecompressor(decompressor);
          decompressor = null;
        }
      }
    }
  }
}

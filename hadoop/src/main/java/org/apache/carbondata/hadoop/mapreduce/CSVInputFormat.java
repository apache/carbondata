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
package org.apache.carbondata.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.carbondata.hadoop.io.BoundedInputStream;
import org.apache.carbondata.hadoop.io.StringArrayWritable;
import org.apache.carbondata.hadoop.util.CSVInputFormatUtil;

import com.univocity.parsers.csv.CsvParser;
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

  @Override
  public RecordReader<NullWritable, StringArrayWritable> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new NewCSVRecordReader();
  }

  /**
   * Treats value as line in file. Key is null.
   */
  public static class NewCSVRecordReader extends RecordReader<NullWritable, StringArrayWritable> {

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
      this.start = split.getStart();
      this.end = this.start + split.getLength();
      Path file = split.getPath();
      Configuration job = context.getConfiguration();
      CompressionCodec codec = (new CompressionCodecFactory(job)).getCodec(file);
      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(file);
      InputStream inputStream = null;
      if (codec != null) {
        this.isCompressedInput = true;
        this.decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
          SplitCompressionInputStream scIn = ((SplittableCompressionCodec) codec)
              .createInputStream(fileIn, this.decompressor, this.start, this.end,
                  SplittableCompressionCodec.READ_MODE.BYBLOCK);
          this.start = scIn.getAdjustedStart();
          this.end = scIn.getAdjustedEnd();
          if (this.start != 0) {
            LineReader lineReader = new LineReader(scIn, 1);
            this.start += lineReader.readLine(new Text(), 0);
          }
          this.filePosition = scIn;
          inputStream = scIn;
        } else {
          CompressionInputStream cIn = codec.createInputStream(fileIn, decompressor);
          this.filePosition = cIn;
          inputStream = cIn;
        }
      } else {
        fileIn.seek(this.start);
        if (this.start != 0) {
          LineReader lineReader = new LineReader(fileIn, 1);
          this.start += lineReader.readLine(new Text(), 0);
        }
        boundedInputStream = new BoundedInputStream(fileIn, this.end - this.start);
        this.filePosition = fileIn;
        inputStream = boundedInputStream;
      }
      reader = new InputStreamReader(inputStream);
      csvParser = new CsvParser(CSVInputFormatUtil.extractCsvParserSettings(job, this.start));
      csvParser.beginParsing(reader);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      this.columns = csvParser.parseNext();
      if (this.columns == null) {
        this.value = null;
        return false;
      }
      if (this.value == null) {
        this.value = new StringArrayWritable();
      }
      this.value.set(this.columns);
      return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public StringArrayWritable getCurrentValue() throws IOException, InterruptedException {
      return this.value;
    }

    private long getPos() throws IOException {
      long retVal = this.start;
      if (null != this.boundedInputStream) {
        retVal = this.end - this.boundedInputStream.getRemaining();
      } else if (this.isCompressedInput && null != this.filePosition) {
        retVal = this.filePosition.getPos();
      }
      return retVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.getPos() -
          this.start) / (float)(this.end - this.start));
    }

    @Override
    public void close() throws IOException {
      try {
        if(this.reader != null) {
          this.reader.close();
        }
      } finally {
        if(this.decompressor != null) {
          CodecPool.returnDecompressor(this.decompressor);
        }
      }
    }
  }
}

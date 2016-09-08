package org.apache.carbondata.hadoop.csvinput;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

/**
 * It uses univocity csv parser to parse the csv file.
 */
public class CsvInputFormat extends FileInputFormat<Void, CustomArrayWritable>
    implements JobConfigurable {

  @Override
  public RecordReader<Void, CustomArrayWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new CSVRecordReader(split, job);
  }

  @Override public void configure(JobConf job) {

  }

  @Override public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] splits = super.getSplits(job, numSplits);
    return splits;
  }

  public static class CSVRecordReader implements RecordReader<Void, CustomArrayWritable> {
    private static final Log LOG = LogFactory.getLog(CSVRecordReader.class);
    private long start;
    private long end;
    private FSDataInputStream fileIn;
    private CustomReader reader;
    private CsvParser csvParser;

    public CSVRecordReader(InputSplit genericSplit, JobConf job) throws IOException {
      initialize(genericSplit, job);
    }

    public void initialize(InputSplit genericSplit, JobConf job) throws IOException {
      FileSplit split = (FileSplit) genericSplit;
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();

      // open the file and seek to the start of the split
      final FileSystem fs = file.getFileSystem(job);
      fileIn = fs.open(file);
      if(start != 0) {
        start -= 1;
      }
      fileIn.seek(start);
      // If this is not the first split, we always throw away first record
      // because we always (except the last split) read one extra line in
      // next() method.
      if (start != 0) {
        LineReader lineReader = new LineReader(fileIn, 1);
        start += lineReader.readLine(new Text(), 0);
      }
      reader = new CustomReader(new BufferedReader(new InputStreamReader(fileIn)));
      reader.setLimit(end - start);
      CsvParserSettings settings = new CsvParserSettings();
      csvParser = new CsvParser(settings);
      csvParser.beginParsing(reader);
    }

    @Override public boolean next(Void key, CustomArrayWritable value) throws IOException {
      String[] columns = csvParser.parseNext();
      if (columns == null) {
        return false;
      }
      value.set(columns);
      return true;
    }

    @Override public Void createKey() {
      return null;
    }

    @Override public CustomArrayWritable createValue() {
      return new CustomArrayWritable();
    }

    @Override public long getPos() throws IOException {
      return end - reader.getRemaining();
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() throws IOException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (reader.getRemaining()) / (float) (end - start));
      }
    }

    public synchronized void close() throws IOException {
      csvParser.stopParsing();
      reader.close();
    }
  }
}

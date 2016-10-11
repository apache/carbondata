package org.apache.carbondata.processing.newflow.iterators;

import java.io.IOException;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.hadoop.mapred.RecordReader;

/**
 * This iterator iterates RecordReader.
 */
public class RecordReaderIterator extends CarbonIterator<Object[]> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RecordReaderIterator.class.getName());

  private RecordReader<Void, CarbonArrayWritable> recordReader;

  private CarbonArrayWritable data = new CarbonArrayWritable();

  public RecordReaderIterator(RecordReader<Void, CarbonArrayWritable> recordReader) {
    this.recordReader = recordReader;
  }

  @Override public boolean hasNext() {
    try {
      return recordReader.next(null, data);
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return false;
  }

  @Override public Object[] next() {
    return data.get();
  }

}

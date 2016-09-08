package org.carbondata.processing.newflow.input;

import java.io.IOException;
import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;

import org.apache.hadoop.mapred.RecordReader;
import org.carbondata.processing.newflow.CarbonArrayWritable;
import org.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.carbondata.processing.newflow.DataField;
import org.carbondata.processing.newflow.DataLoadProcessorStep;
import org.carbondata.processing.newflow.parser.CarbonParserFactory;
import org.carbondata.processing.newflow.parser.GenericParser;

/**
 * It is wrapper class around input format
 */
public class InputProcessorStepImpl implements DataLoadProcessorStep {


  private RecordReader<Void, CarbonArrayWritable> recordReader;

  private CarbonDataLoadConfiguration configuration;

  private GenericParser[] genericParsers;

  @Override public DataField[] getOutput() {
    DataField[] fields = configuration.getDataFields();
    String[] header = configuration.getHeader();
    DataField[] output = new DataField[fields.length];
    int k = 0;
    for (int i = 0; i < header.length; i++) {
      for (int j = 0; j < fields.length; j++) {
        if (header[j].equalsIgnoreCase(fields[j].getColumn().getColName())) {
          output[k++] = fields[j];
          break;
        }
      }
    }
    return output;
  }

  @Override
  public void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) {
    this.recordReader = configuration.getRecordReader();
    this.configuration = configuration;
    DataField[] output = getOutput();
    genericParsers = new GenericParser[output.length];
    for (int i = 0; i < genericParsers.length; i++) {
      genericParsers[i] = CarbonParserFactory.createParser(output[i].getColumn(), configuration.getComplexDelimiters());
    }

    if(child != null) {
      child.intialize(configuration, child);
    }
  }

  @Override public Iterator<Object[]> execute() {

    return new CarbonIterator<Object[]>() {
      CarbonArrayWritable data = new CarbonArrayWritable();
      @Override public boolean hasNext() {
        try {
          return recordReader.next(null, data);
        } catch (IOException e) {
          e.printStackTrace();
        }
        return false;
      }

      @Override public Object[] next() {

        return data.get();
      }
    };
  }

  @Override public void close() {
    try {
      this.recordReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

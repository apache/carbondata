package org.apache.carbondata.processing.newflow.steps.encodestep;

import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.DataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.encoding.RowEncoder;
import org.apache.carbondata.processing.newflow.encoding.impl.RowEncoderImpl;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

/**
 * Encode data with dictionary values and composed with bit/byte packed key.
 * nondictionary values are packed as bytes, and complex types are also packed as bytes.
 */
public class EncoderProcessorStepImpl implements DataLoadProcessorStep {

  private CarbonDataLoadConfiguration configuration;

  private DataLoadProcessorStep child;

  private RowEncoder encoder;

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override
  public void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child)
      throws CarbonDataLoadingException {
    this.configuration = configuration;
    this.child = child;
    encoder = new RowEncoderImpl(child.getOutput(), configuration);
    child.intialize(configuration, child);
  }

  @Override public Iterator<Object[]> execute() throws CarbonDataLoadingException {
    final Iterator<Object[]> iterator = child.execute();
    return new CarbonIterator<Object[]>() {
      @Override public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override public Object[] next() {
        Object[] next = iterator.next();
        return encoder.encode(next);
      }
    };
  }

  @Override public void finish() {
    encoder.finish();
  }
}

package org.carbondata.processing.newflow.mdkstep;

import java.util.Iterator;

import org.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.carbondata.processing.newflow.DataField;
import org.carbondata.processing.newflow.DataLoadProcessorStep;

public class MDKGeneratorProcessorStepImpl implements DataLoadProcessorStep {

  @Override public DataField[] getOutput() {
    return new DataField[0];
  }

  @Override
  public void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) {

  }

  @Override public Iterator<Object[]> execute() {
    return null;
  }

  @Override public void close() {

  }
}

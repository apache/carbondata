package org.carbondata.processing.newflow;

import java.util.Iterator;

public interface DataLoadProcessorStep {

  DataField[] getOutput();

  void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child);

  Iterator<Object[]> execute();

  void close();

}

package org.apache.carbondata.processing.newflow;

import java.util.Iterator;

import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

public interface DataLoadProcessorStep {

  DataField[] getOutput();

  void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) throws
      CarbonDataLoadingException;

  Iterator<Object[]> execute() throws CarbonDataLoadingException;

  void close();

}

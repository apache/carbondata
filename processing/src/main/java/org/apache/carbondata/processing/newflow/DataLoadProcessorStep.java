package org.apache.carbondata.processing.newflow;

import java.util.Iterator;

import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

/**
 * This base interface for data loading. It can do transformation jobs as per the implementation.
 *
 */
public interface DataLoadProcessorStep {

  /**
   * The output meta for this step. The data returns from this step is as per this meta.
   * @return
   */
  DataField[] getOutput();

  /**
   * Intialization process for this step.
   * @param configuration
   * @param child
   * @throws CarbonDataLoadingException
   */
  void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) throws
      CarbonDataLoadingException;

  /**
   * Tranform the data as per the implemetation.
   * @return Iterator of data
   * @throws CarbonDataLoadingException
   */
  Iterator<Object[]> execute() throws CarbonDataLoadingException;

  /**
   * Any closing of resources after step execution can be done here.
   */
  void finish();

}

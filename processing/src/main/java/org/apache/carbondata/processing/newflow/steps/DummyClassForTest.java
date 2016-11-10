package org.apache.carbondata.processing.newflow.steps;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;

/**
 * DummyClassForTest
 */
public class DummyClassForTest extends AbstractDataLoadProcessorStep {

  private ExecutorService executorService;

  public DummyClassForTest(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws CarbonDataLoadingException {

  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    this.executorService = Executors.newFixedThreadPool(iterators.length);

    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.submit(new DummyThread(iterators[i]));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    return null;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }
}

/**
 * This thread iterates the iterator
 */
class DummyThread implements Callable<Void> {

  private Iterator<CarbonRowBatch> iterator;

  public DummyThread(Iterator<CarbonRowBatch> iterator) {
    this.iterator = iterator;
  }

  @Override public Void call() throws CarbonDataLoadingException {
    try {
      while (iterator.hasNext()) {
        CarbonRowBatch batch = iterator.next();
        Iterator<CarbonRow> batchIterator = batch.getBatchIterator();
        while (batchIterator.hasNext()) {
          CarbonRow row = batchIterator.next();
          // do nothing
        }
      }

    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
    return null;
  }
}

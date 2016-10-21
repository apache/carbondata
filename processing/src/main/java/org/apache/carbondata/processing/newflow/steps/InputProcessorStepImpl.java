package org.apache.carbondata.processing.newflow.steps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.parser.CarbonParserFactory;
import org.apache.carbondata.processing.newflow.parser.GenericParser;
import org.apache.carbondata.processing.newflow.parser.RowParser;
import org.apache.carbondata.processing.newflow.parser.impl.RowParserImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;

/**
 * It reads data from record reader and sends data to next step.
 */
public class InputProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(InputProcessorStepImpl.class.getName());

  private GenericParser[] genericParsers;

  private List<Iterator<Object[]>> inputIterators;

  public InputProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child, List<Iterator<Object[]>> inputIterators) {
    super(configuration, child);
    this.inputIterators = inputIterators;
  }

  @Override
  public DataField[] getOutput() {
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
  public void initialize() throws CarbonDataLoadingException {
    DataField[] output = getOutput();
    genericParsers = new GenericParser[output.length];
    for (int i = 0; i < genericParsers.length; i++) {
      genericParsers[i] = CarbonParserFactory.createParser(output[i].getColumn(),
          (String[]) configuration
              .getDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS));
    }
  }



  @Override
  public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<Iterator<Object[]>>[] readerIterators = partitionInputReaderIterators();
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] = new InputProcessorIterator(readerIterators[i], genericParsers, batchSize);
    }
    return outIterators;
  }

  /**
   * Partition input iterators equally as per the number of threads.
   * @return
   */
  private List<Iterator<Object[]>>[] partitionInputReaderIterators() {
    // Get the number of cores configured in property.
    int numberOfCores = CarbonProperties.getInstance().getNumberOfCores();
    // Get the minimum of number of cores and iterators size to get the number of parallel threads
    // to be launched.
    int parallelThreadNumber = Math.min(inputIterators.size(), numberOfCores);

    List<Iterator<Object[]>>[] iterators = new List[parallelThreadNumber];
    for (int i = 0; i < parallelThreadNumber; i++) {
      iterators[i] = new ArrayList<>();
    }
    // Equally partition the iterators as per number of threads
    for (int i = 0; i < inputIterators.size(); i++) {
      iterators[i % parallelThreadNumber].add(inputIterators.get(i));
    }
    return iterators;
  }

  @Override
  protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  /**
   * This iterator wraps the list of iterators and it starts iterating the each
   * iterator of the list one by one. It also parse the data while iterating it.
   */
  private static class InputProcessorIterator extends CarbonIterator<CarbonRowBatch> {

    private List<Iterator<Object[]>> inputIterators;

    private Iterator<Object[]> currentIterator;

    private int counter;

    private int batchSize;

    private RowParser rowParser;

    public InputProcessorIterator(List<Iterator<Object[]>> inputIterators,
        GenericParser[] genericParsers, int batchSize) {
      this.inputIterators = inputIterators;
      this.batchSize = batchSize;
      this.rowParser = new RowParserImpl(genericParsers);
      this.counter = 0;
      // Get the first iterator from the list.
      currentIterator = inputIterators.get(counter++);
    }

    @Override
    public boolean hasNext() {
      return internalHasNext();
    }

    private boolean internalHasNext() {
      boolean hasNext = currentIterator.hasNext();
      // If iterator is finished then check for next iterator.
      if (!hasNext) {
        // Check next iterator is available in the list.
        if (counter < inputIterators.size()) {
          // Get the next iterator from the list.
          currentIterator = inputIterators.get(counter++);
        }
        hasNext = internalHasNext();
      }
      return hasNext;
    }

    @Override
    public CarbonRowBatch next() {
      // Create batch and fill it.
      CarbonRowBatch carbonRowBatch = new CarbonRowBatch();
      int count = 0;
      while (internalHasNext() && count < batchSize) {
        carbonRowBatch.addRow(new CarbonRow(rowParser.parseRow(currentIterator.next())));
        count++;
      }
      return carbonRowBatch;
    }
  }

}

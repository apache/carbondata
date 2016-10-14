package org.apache.carbondata.processing.newflow.steps.input;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.parser.CarbonParserFactory;
import org.apache.carbondata.processing.newflow.parser.GenericParser;
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

  @Override public void intialize() throws CarbonDataLoadingException {
    DataField[] output = getOutput();
    genericParsers = new GenericParser[output.length];
    for (int i = 0; i < genericParsers.length; i++) {
      genericParsers[i] = CarbonParserFactory.createParser(output[i].getColumn(),
          (String[]) configuration
              .getDataLoadProperty(DataLoadProcessorConstants.COMPLEX_DELIMITERS));
    }
  }

  private int getNumberOfCores() {
    int numberOfCores;
    try {
      numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException exc) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    return numberOfCores;
  }

  private int getBatchSize() {
    int batchSize;
    try {
      batchSize = Integer.parseInt(configuration
          .getDataLoadProperty(DataLoadProcessorConstants.DATA_LOAD_BATCH_SIZE,
              DataLoadProcessorConstants.DATA_LOAD_BATCH_SIZE_DEFAULT).toString());
    } catch (NumberFormatException exc) {
      batchSize = Integer.parseInt(DataLoadProcessorConstants.DATA_LOAD_BATCH_SIZE_DEFAULT);
    }
    return batchSize;
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = getBatchSize();
    List<Iterator<Object[]>>[] readerIterators = partitionInputReaderIterators();
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] = new InputProcessorIterator(readerIterators[i], genericParsers, batchSize);
    }
    return outIterators;
  }

  private List<Iterator<Object[]>>[] partitionInputReaderIterators() {
    int numberOfCores = getNumberOfCores();
    if (inputIterators.size() < numberOfCores) {
      numberOfCores = inputIterators.size();
    }
    List<Iterator<Object[]>>[] iterators = new List[numberOfCores];
    for (int i = 0; i < numberOfCores; i++) {
      iterators[i] = new ArrayList<>();
    }

    for (int i = 0; i < inputIterators.size(); i++) {
      iterators[i % numberOfCores].add(inputIterators.get(i));

    }
    return iterators;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  private static class InputProcessorIterator extends CarbonIterator<CarbonRowBatch> {

    private List<Iterator<Object[]>> inputIterators;

    private GenericParser[] genericParsers;

    private Iterator<Object[]> currentIterator;

    private int counter;

    private int batchSize;

    public InputProcessorIterator(List<Iterator<Object[]>> inputIterators,
        GenericParser[] genericParsers, int batchSize) {
      this.inputIterators = inputIterators;
      this.genericParsers = genericParsers;
      this.batchSize = batchSize;
      currentIterator = inputIterators.get(counter++);
    }

    @Override public boolean hasNext() {
      return internalHasNext();
    }

    private boolean internalHasNext() {
      boolean hasNext = currentIterator.hasNext();
      if (!hasNext) {
        if (counter < inputIterators.size()) {
          currentIterator = inputIterators.get(counter++);
        }
        hasNext = internalHasNext();
      }
      return hasNext;
    }

    @Override public CarbonRowBatch next() {
      CarbonRowBatch carbonRowBatch = new CarbonRowBatch();
      int count = 0;
      while (internalHasNext() && count < batchSize) {
        Object[] row = currentIterator.next();
        for (int i = 0; i < row.length; i++) {
          row[i] = genericParsers[i].parse(row[i].toString());
        }
        carbonRowBatch.addRow(new CarbonRow(row));
        count++;
      }
      return carbonRowBatch;
    }
  }

}

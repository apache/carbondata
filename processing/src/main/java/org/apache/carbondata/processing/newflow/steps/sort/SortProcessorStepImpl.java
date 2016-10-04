package org.apache.carbondata.processing.newflow.steps.sort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.DataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

public class SortProcessorStepImpl implements DataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortProcessorStepImpl.class.getName());

  private DataLoadProcessorStep child;

  private SortDataRows sortDataRows;

  private CarbonDataLoadConfiguration configuration;

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override
  public void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) throws CarbonDataLoadingException {
   this.child = child;
    this.configuration = configuration;
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    this.sortDataRows = new SortDataRows(
        tableIdentifier.getTableName(),
        configuration.getDimensionCount() - configuration.getComplexDimensionCount(),
        configuration.getComplexDimensionCount(), configuration.getMeasureCount(), new SortObserver(),
        configuration.getNoDictionaryCount(), configuration.getPartitionId(), configuration.getSegmentId() + "",
        configuration.getTaskNo(), getNoDictionaryMapping(configuration.getDataFields()));
    try {
      // initialize sort
      this.sortDataRows.initialize(tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  @Override public Iterator<Object[]> execute() throws CarbonDataLoadingException {
    final Iterator<Object[]> iterator = child.execute();
    try {
      while (iterator.hasNext()) {
        sortDataRows.addRow(iterator.next());
      }
      processRowToNextStep();
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new CarbonDataLoadingException(e);
    }

    return new CarbonIterator<Object[]>() {
      @Override public boolean hasNext() {
        return false;
      }

      @Override public Object[] next() {
        return new Object[0];
      }
    };
  }

  /**
   * Below method will be used to process data to next step
   *
   */
  private boolean processRowToNextStep() throws CarbonDataLoadingException {
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    if (null == this.sortDataRows) {
      LOGGER.info("Record Processed For table: " + tableIdentifier.getTableName());
      LOGGER.info("Number of Records was Zero");
      String logMessage = "Summary: Carbon Sort Key Step: Read: " + 0 + ": Write: " + 0;
      LOGGER.info(logMessage);
      return false;
    }

    try {
      // start sorting
      this.sortDataRows.startSorting();

      // check any more rows are present
      LOGGER.info("Record Processed For table: " + tableIdentifier.getTableName());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordSortRowsStepTotalTime(
          configuration.getPartitionId(), System.currentTimeMillis());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordDictionaryValuesTotalTime(
          configuration.getPartitionId(), System.currentTimeMillis());
      return false;
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }

  }

  @Override public void close() {

  }

  /**
   * Preparing the boolean [] to map whether the dimension is no Dictionary or not.
   *
   */
  private boolean[] getNoDictionaryMapping(DataField[] fields) {
    // boolean[] NoDictionaryMapping = new boolean[dims.size()];
    List<Boolean> noDictionaryMapping = new ArrayList<Boolean>();
    for (DataField field : fields) {
      // for  complex type need to break the loop
      if (field.getColumn().isComplex()) {
        break;
      }

      if (!field.hasDictionaryEncoding() && field.getColumn().isDimesion()) {
        noDictionaryMapping.add(true);
      } else if (field.getColumn().isDimesion()){
        noDictionaryMapping.add(false);
      }
    }
    return ArrayUtils.toPrimitive(noDictionaryMapping.toArray(new Boolean[noDictionaryMapping.size()]));
  }
}

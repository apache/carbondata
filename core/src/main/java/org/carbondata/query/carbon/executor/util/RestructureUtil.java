package org.carbondata.query.carbon.executor.util;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.carbon.executor.infos.AggregatorInfo;

public class RestructureUtil {

  /**
   * Below method will be used to get the updated query dimension updation
   * means, after restructuring some dimension will be not present in older
   * table blocks in that case we need to select only those dimension out of
   * query dimension which is present in the current table block
   *
   * @param queryDimensions
   * @param tableBlockDimensions
   * @return list of query dimension which is present in the table block
   */
  public static List<CarbonDimension> getUpdatedQueryDimension(
      List<CarbonDimension> queryDimensions, List<CarbonDimension> tableBlockDimensions) {
    List<CarbonDimension> presentDimension =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // selecting only those dimension which is present in the query
    for (CarbonDimension queryDimimension : queryDimensions) {
      if (tableBlockDimensions.equals(queryDimimension)) {
        presentDimension.add(queryDimimension);
      }
    }
    return presentDimension;
  }

  /**
   * Below method is to add dimension children for complex type dimension as
   * internally we are creating dimension column for each each complex
   * dimension so when complex query dimension request will come in the query,
   * we need to add its children as it is hidden from the user For example if
   * complex dimension is of Array of String[2] so we are storing 3 dimension
   * and when user will query for complex type i.e. array type we need to add
   * its children and then we will read respective block and create a tuple
   * based on all three dimension
   *
   * @param queryDimensions      current query dimensions
   * @param tableBlockDimensions dimensions which is present in the table block
   * @return updated dimension(after adding complex type children)
   */
  public static List<CarbonDimension> addChildrenForComplexTypeDimension(
      List<CarbonDimension> queryDimensions, List<CarbonDimension> tableBlockDimensions) {
    List<CarbonDimension> updatedQueryDimension = new ArrayList<CarbonDimension>();
    int numberOfChildren = 0;
    for (CarbonDimension queryDimension : queryDimensions) {
      // if number of child is zero, then it is not a complex dimension
      // so directly add it query dimension
      if (queryDimension.numberOfChild() == 0) {
        updatedQueryDimension.add(queryDimension);
      }
      // if number of child is more than 1 then add all its children
      numberOfChildren = queryDimension.getOrdinal() + queryDimension.numberOfChild();
      for (int j = queryDimension.getOrdinal(); j < numberOfChildren; j++) {
        updatedQueryDimension.add(tableBlockDimensions.get(j));
      }
    }
    return updatedQueryDimension;
  }

  /**
   * Below method will be used to get the aggregator info object
   * in this method some of the properties which will be extracted
   * from query measure and current block measures will be set
   *
   * @param queryMeasures        measures present in query
   * @param currentBlockMeasures current block measures
   * @return aggregator info
   */
  public static AggregatorInfo getAggregatorInfos(List<CarbonMeasure> queryMeasures,
      List<CarbonMeasure> currentBlockMeasures) {
    AggregatorInfo aggregatorInfos = new AggregatorInfo();
    int numberOfMeasureInQuery = queryMeasures.size();
    int[] measureOrdinals = new int[numberOfMeasureInQuery];
    Object[] defaultValues = new Object[numberOfMeasureInQuery];
    boolean[] measureExistsInCurrentBlock = new boolean[numberOfMeasureInQuery];
    int index = 0;
    for (CarbonMeasure queryMeasure : queryMeasures) {
      measureOrdinals[index] = queryMeasure.getOrdinal();
      // if query measure exists in current dimension measures
      // then setting measure exists is true
      // otherwise adding a default value of a measure
      if (currentBlockMeasures.contains(queryMeasure)) {
        measureExistsInCurrentBlock[index] = true;
      } else {
        defaultValues[index] = queryMeasure.getDefaultValue();
      }
      index++;
    }
    aggregatorInfos.setDefaultValues(defaultValues);
    aggregatorInfos.setMeasureOrdinals(measureOrdinals);
    aggregatorInfos.setMeasureExists(measureExistsInCurrentBlock);
    return aggregatorInfos;
  }
}

package org.apache.carbondata.core.datamap.dev;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;

public class DataMapChooser {

  private Expression srcExpression;

  private AbsoluteTableIdentifier identifier;

  public DataMapChooser(Expression srcExpression, AbsoluteTableIdentifier identifier) {
    this.srcExpression = srcExpression;
    this.identifier = identifier;
  }

  public DataMapExpression dataMapSelection() {
    DataMapCoveringOptions.DataMapCovered DataMapCovered;
    Map<DataMapFactory, List<DataMapColumnExpression>> dataMapCoverOption = new HashMap<>();
    DataMapExpression dataMapExpression = null;

    DataMapCovered = dataMapConvered(dataMapCoverOption);

    if (DataMapCovered.equals(DataMapCoveringOptions.DataMapCovered.FULLY_COVERED) || DataMapCovered
        .equals(DataMapCoveringOptions.DataMapCovered.PARTIALLY_COVERED)) {
      // Only one data Map factory. Form the tree based on that.
      DataMapExpressionTree dataMapExpressionTree =
          new DataMapExpressionTree(DataMapCovered, dataMapCoverOption);
      dataMapExpression = dataMapExpressionTree.dataMapExpressionTreeFormation();
    }

    return dataMapExpression;
  }

  public DataMapCoveringOptions.DataMapCovered dataMapConvered(
      Map<DataMapFactory, List<DataMapColumnExpression>> dataMapChoosen) {
    // Get list of dataMaps.
    List<TableDataMap> tableDataMaps =
        DataMapStoreManager.getInstance().getAllDataMap(this.identifier);

    if (tableDataMaps != null) {
      for (TableDataMap tableDataMap : tableDataMaps) {
        DataMapFactory factory = tableDataMap.getDataMapFactory();

        // DataMapColumnExpression dataMapColumnExpression = new DataMapColumnExpression(this.srcExpression);
        Map<DataMapCoveringOptions.DataMapCovered, List<DataMapColumnExpression>> coverage =
            checkCoverage(factory.getMeta());
        if (coverage.keySet().contains(DataMapCoveringOptions.DataMapCovered.FULLY_COVERED)) {
          // Fully covered. So choose this dataMap.
          dataMapChoosen.put(factory, new ArrayList<>(
              coverage.get(DataMapCoveringOptions.DataMapCovered.FULLY_COVERED)));
          return DataMapCoveringOptions.DataMapCovered.FULLY_COVERED;
        } else if (coverage.keySet()
            .contains(DataMapCoveringOptions.DataMapCovered.PARTIALLY_COVERED)) {
          dataMapChoosen.put(factory, new ArrayList<>(
              coverage.get(DataMapCoveringOptions.DataMapCovered.PARTIALLY_COVERED)));
        }
      }
    }
    if (dataMapChoosen.size() > 0) {
      return DataMapCoveringOptions.DataMapCovered.PARTIALLY_COVERED;
    }
    return DataMapCoveringOptions.DataMapCovered.NOT_COVERED;
  }


  private Map<DataMapCoveringOptions.DataMapCovered, List<DataMapColumnExpression>> checkCoverage(
      DataMapMeta meta) {
    // Check if the coverage.
    DataMapEvaluator dataMapEvaluator = new DataMapEvaluator(srcExpression);
    return dataMapEvaluator.expressionEvaluator(meta);
  }

  private void getDataMapList(AbsoluteTableIdentifier identifier) {
  }
}

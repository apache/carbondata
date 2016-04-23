package org.carbondata.query.carbon.model;

import java.io.Serializable;
import java.util.List;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;

public class DimensionAggregatorInfo implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 4801602263271340969L;

  /**
   * name of the column in which aggregation is applied
   */
  private String columnName;

  /**
   * dimension in which aggregation is applied
   */
  private CarbonDimension dim;

  /**
   * list if aggregate function applied in the dimension
   */
  private List<String> aggList;

  /**
   * order of the aggregate function in which output of aggregation will be
   * send from executor to driver. Integer represents the order of the output
   */
  private List<Integer> orderOfAggregation;

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @param columnName the columnName to set
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  /**
   * @return the dim
   */
  public CarbonDimension getDim() {
    return dim;
  }

  /**
   * @param dim the dim to set
   */
  public void setDim(CarbonDimension dim) {
    this.dim = dim;
  }

  /**
   * @return the aggList
   */
  public List<String> getAggList() {
    return aggList;
  }

  /**
   * @param aggList the aggList to set
   */
  public void setAggList(List<String> aggList) {
    this.aggList = aggList;
  }

  /**
   * @return the orderList
   */
  public List<Integer> getOrderList() {
    return orderOfAggregation;
  }

  /**
   * @param orderList the orderList to set
   */
  public void setOrderList(List<Integer> orderList) {
    this.orderOfAggregation = orderList;
  }
}

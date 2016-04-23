package org.carbondata.query.carbon.executor.infos;

import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * Info class which store all the details
 * which is required during aggregation
 */
public class AggregatorInfo {

  /**
   * measure aggregator array instance which will be used to
   * aggregate the aggregate function columns
   * it can be any dimension or measure column
   */
  private MeasureAggregator[] measuresAggreagators;

  /**
   * selected query measure ordinal
   * which will be used to read the measures chunk data
   * this will be storing the index of the measure in measures chunk
   */
  private int[] measureOrdinals;

  /**
   * This parameter will be used to
   * check whether particular measure is present
   * in the table block, if not then its default value will be used
   */
  private boolean[] measureExists;

  /**
   * this default value will be used to when some measure is not present
   * in the table block, in case of restructuring of the table if user is adding any
   * measure then in older block that measure wont be present so for measure default value
   * will be used to aggregate in the older table block query execution
   */
  private Object[] defaultValues;

  /**
   * In carbon there are three type of aggregation
   * (dimension aggregation, expression aggregation and measure aggregation)
   * Below index will be used to set the start position of expression in measures
   * aggregator array
   */
  private int expressionAggregatorStartIndex;

  /**
   *In carbon there are three type of aggregation
   *(dimension aggregation, expression aggregation and measure aggregation)
   *Below index will be used to set the start position of measures in measures
   *aggregator array
   */
  private int measureAggregatorStartIndex;

  /**
   * @return the measuresAggreagators
   */
  public MeasureAggregator[] getMeasuresAggreagators() {
    return measuresAggreagators;
  }

  /**
   * @param measuresAggreagators the measuresAggreagators to set
   */
  public void setMeasuresAggreagators(MeasureAggregator[] measuresAggreagators) {
    this.measuresAggreagators = measuresAggreagators;
  }

  /**
   * @return the measureOrdinal
   */
  public int[] getMeasureOrdinals() {
    return measureOrdinals;
  }

  /**
   * @param measureOrdinal the measureOrdinal to set
   */
  public void setMeasureOrdinals(int[] measureOrdinal) {
    this.measureOrdinals = measureOrdinal;
  }

  /**
   * @return the measureExists
   */
  public boolean[] getMeasureExists() {
    return measureExists;
  }

  /**
   * @param measureExists the measureExists to set
   */
  public void setMeasureExists(boolean[] measureExists) {
    this.measureExists = measureExists;
  }

  /**
   * @return the defaultValues
   */
  public Object[] getDefaultValues() {
    return defaultValues;
  }

  /**
   * @param defaultValues the defaultValues to set
   */
  public void setDefaultValues(Object[] defaultValues) {
    this.defaultValues = defaultValues;
  }

  /**
   * @return the expressionAggregatorStartIndex
   */
  public int getExpressionAggregatorStartIndex() {
    return expressionAggregatorStartIndex;
  }

  /**
   * @param expressionAggregatorStartIndex the expressionAggregatorStartIndex to set
   */
  public void setExpressionAggregatorStartIndex(int expressionAggregatorStartIndex) {
    this.expressionAggregatorStartIndex = expressionAggregatorStartIndex;
  }

  /**
   * @return the measureAggregatorStartIndex
   */
  public int getMeasureAggregatorStartIndex() {
    return measureAggregatorStartIndex;
  }

  /**
   * @param measureAggregatorStartIndex the measureAggregatorStartIndex to set
   */
  public void setMeasureAggregatorStartIndex(int measureAggregatorStartIndex) {
    this.measureAggregatorStartIndex = measureAggregatorStartIndex;
  }
}

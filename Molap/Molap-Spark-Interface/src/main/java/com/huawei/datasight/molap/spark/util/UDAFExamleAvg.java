package com.huawei.datasight.molap.spark.util;
//
//import org.apache.hadoop.hive.ql.exec.UDAF;
//import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
//
///**
// * This is a simple UDAF that calculates average.
// * 
// * It should be very easy to follow and can be used as an example for writing
// * new UDAFs.
// * 
// * Note that Hive internally uses a different mechanism (called GenericUDAF) to
// * implement built-in aggregation functions, which are harder to program but
// * more efficient.
// * 
// */
public final class UDAFExamleAvg {//extends UDAF {
//
//  /**
//   * The internal state of an aggregation for average.
//   * 
//   * Note that this is only needed if the internal state cannot be represented
//   * by a primitive.
//   * 
//   * The internal state can also contains fields with types like
//   * ArrayList<String> and HashMap<String,Double> if needed.
//   */
//  public static class UDAFAvgState {
//    private long mCount;
//    private double mSum;
//  }
//
//  /**
//   * The actual class for doing the aggregation. Hive will automatically look
//   * for all internal classes of the UDAF that implements UDAFEvaluator.
//   */
//  public static class UDAFExampleAvgEvaluator implements UDAFEvaluator {
//
//    UDAFAvgState state;
//
//    public UDAFExampleAvgEvaluator() {
//      super();
//      state = new UDAFAvgState();
//      init();
//    }
//
//    /**
//     * Reset the state of the aggregation.
//     */
//    public void init() {
//      state.mSum = 0;
//      state.mCount = 0;
//    }
//
//    /**
//     * Iterate through one row of original data.
//     * 
//     * The number and type of arguments need to the same as we call this UDAF
//     * from Hive command line.
//     * 
//     * This function should always return true.
//     */
//    public boolean iterate(Double o) {
//      if (o != null) {
//        state.mSum += o;
//        state.mCount++;
//      }
//      return true;
//    }
//
//    /**
//     * Terminate a partial aggregation and return the state. If the state is a
//     * primitive, just return primitive Java classes like Integer or String.
//     */
//    public UDAFAvgState terminatePartial() {
//      // This is SQL standard - average of zero items should be null.
//      return state.mCount == 0 ? null : state;
//    }
//
//    /**
//     * Merge with a partial aggregation.
//     * 
//     * This function should always have a single argument which has the same
//     * type as the return value of terminatePartial().
//     */
//    public boolean merge(UDAFAvgState o) {
//      if (o != null) {
//        state.mSum += o.mSum;
//        state.mCount += o.mCount;
//      }
//      return true;
//    }
//
//    /**
//     * Terminates the aggregation and return the final result.
//     */
//    public Double terminate() {
//      // This is SQL standard - average of zero items should be null.
//      return state.mCount == 0 ? null : Double.valueOf(state.mSum
//          / state.mCount);
//    }
//  }
//
//  private UDAFExamleAvg() {
//    // prevent instantiation
//  }
//
}

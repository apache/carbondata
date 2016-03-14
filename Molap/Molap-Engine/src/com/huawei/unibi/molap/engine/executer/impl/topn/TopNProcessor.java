/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7F5s+pCylBO6lI15rXo/DgaCJ4bMtlzt7EaUMtGwWmbx5reWY3uCQg8RUdqDYet1cdIG
XQOIXYt+7rE9W1ylLTuahMaGzkac3ZSJFM/z7mYYeqNe3NzuPzpPv4yPXDZO4g==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.executer.impl.topn;


/**
 * This class calculates the topN rows as per the defined parameters.
 * 
 * @author R00900208
 *
 */
public class TopNProcessor
{
//    
////    /**
////     * It calculates and returns the topN rows.
////     * @param result
////     * @param dimIndex
////     * @param topMeasureIndex
////     * @param topNCount
////     * @param dimCount
////     * @param topNType
////     * @return
////     */
////    public static double[][] getTopNRows(double[][] result, int dimIndex, int topMeasureIndex, int topNCount,
////            int dimCount, MolapTopNType topNType, String aggName, int countMsrIndex, int avgMsrIndex)
////    {
////        List<TopNHolderGroup> groups = new ArrayList<TopNProcessor.TopNHolderGroup>();
////        
////        TopNHolderGroup group = new TopNHolderGroup(dimIndex,topNCount,topNType);
////        groups.add(group);
////        TopNHolder holder = new TopNHolder(dimIndex, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex);
////     
////        for(int i = 0;i < result.length;i++)
////        {
////            
////            if(!holder.addRow(result[i]))
////            {
////                if(!group.addHolder(holder))
////                {
////                    group = new TopNHolderGroup(dimIndex,topNCount,topNType);
////                    groups.add(group);
////                    group.addHolder(holder);
////                }
////                holder = new TopNHolder(dimIndex, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex);
////                holder.addRow(result[i]);
////            }
////        }
////        
////        if(holder.rows.size() > 0)
////        {
////            if(!group.addHolder(holder))
////            {
////                group = new TopNHolderGroup(dimIndex,topNCount,topNType);
////                groups.add(group);
////                group.addHolder(holder);
////            }
////        }
////        
////        List<double[]> topNRows = new ArrayList<double[]>();
////        for(TopNHolderGroup holderGroup : groups)
////        {
////            AbstractQueue<TopNHolder> holders = holderGroup.getHolders();
////            
////            int size = holders.size();
////            List<TopNHolder> list = new ArrayList<TopNProcessor.TopNHolder>();
////            for(int i = 0;i < size;i++)
////            {
////                list.add(holders.poll());
////            }
////            for(int i = list.size()-1;i >= 0;i--)
////            {
////                topNRows.addAll(list.get(i).rows);
////            }
////        }
////        return topNRows.toArray(new double[topNRows.size()][]);
////    }
//    
//    
// 
//    /**
//     * This class holds group of TopN holders which are belonged same group as per the top n count
//     * @author R00900208
//     *
//     */
//    private static final class TopNHolderGroup
//    {
//        /**
//         * topN
//         */
//        private int topN;
//        
//        /**
//         * holders
//         */
//        private AbstractQueue<TopNHolder> holders = null;
//        
//        /**
//         * dimIndex
//         */
//        private int dimIndex;
//        
//        /**
//         * lastHolder
//         */
//        private TopNHolder lastHolder;
//        
//        /**
//         * topNType
//         */
//        private MolapTopNType topNType;
//        
//        /**
//         * Constructor that takes the meta information for this object
//         * @param dimIndex , From where it supposed to do group
//         * @param topN , count of rows
//         * @param topNType, whether top or bottom.
//         */
//        private TopNHolderGroup(int dimIndex,int topN,MolapTopNType topNType)
//        {
//            this.topN = topN;
//            this.dimIndex = dimIndex;
//            this.topNType = topNType;
//            if(topNType.equals(MolapTopNType.TOP))
//            {
//                createTopRecordHolderQueue();
//            }
//            else
//            {
//                createBottomRecordHolderQueue();
//            }
//        }
//        
//        /**
//         * Add the topn holder to this group. 
//         * @param holder
//         * @return, it returns true if it belonged to same group and added to it or else false.
//         */
//        public boolean addHolder(TopNHolder holder)
//        {
//            if(lastHolder == null)
//            {
//                holders.add(holder);
//                lastHolder = holder;
//                return true;
//            }
//            
//            
//            if(equalsObject(lastHolder.lastRow, holder.lastRow))
//            {
//                if(holders.size() >= topN)
//                {
//                    TopNHolder peek = holders.peek();
//                    handleTopNType(holder, peek);
//                }
//                else
//                {
//                    
//                    holders.add(holder);
//                }
//                lastHolder = holder;
//                return true;
//            }
//            return false;
//        }
//
//        private void handleTopNType(TopNHolder holder, TopNHolder peek)
//        {
//            if(topNType.equals(MolapTopNType.TOP))
//            {
//                if(compareTop(holder, peek) > 0)
//                {
//                    holders.poll();
//                    holders.add(holder);
//                }
//            }
//            else
//            {
//                if(compareBottom(holder, peek) > 0)
//                {
//                    holders.poll();
//                    holders.add(holder);
//                }
//            }
//        }
//        
//        /**
//         * Equals check for two arrays.
//         * @param lastRow
//         * @param row
//         * @return
//         */
//        private boolean equalsObject(double[] lastRow,double[] row)
//        {
//            for(int i = 0;i < dimIndex;i++)
//            {
//                if(!(lastRow[i] == row[i]))
//                {
//                    return false;
//                }
//            }
//            return true;
//        }
//        
//        /**
//         * Get all the holders with in this group.
//         * @return
//         */
//        public AbstractQueue<TopNHolder> getHolders()
//        {
//            return holders;
//        }
//        
//        
//        /**
//         * This method will be used to create the record holder heap
//         * 
//         * 
//         */
//        private void createTopRecordHolderQueue()
//        {
//            holders = new PriorityQueue<TopNHolder>(topN, new Comparator<TopNHolder>()
//            {
//                /**
//                 * compare method to compare to row
//                 * 
//                 * @param r1
//                 *            row 1
//                 * @param r2
//                 *            row 2
//                 * @return compare result
//                 * 
//                 */
//                public int compare(TopNHolder r1, TopNHolder r2)
//                {
//                    return compareTop(r1, r2);
//                }
//            });
//        }
//        
//        private int compareTop(TopNHolder r1, TopNHolder r2)
//        {
//            if(r1.agg.getValue() > r2.agg.getValue())
//            {
//                return 1;
//            }
//            if(r1.agg.getValue() < r2.agg.getValue())
//            {
//                return -1;
//            }
//            
//            return 0;
//        }
//        
//        /**
//         * compare method to compare to row
//         * 
//         * @param r1
//         *            row 1
//         * @param r2
//         *            row 2
//         * @return compare result
//         * 
//         */
//        private int compareBottom(TopNHolder r1, TopNHolder r2)
//        {
//            if(r1.agg.getValue() > r2.agg.getValue())
//            {
//                return -1;
//            }
//            if(r1.agg.getValue() < r2.agg.getValue())
//            {
//                return 1;
//            }
//            
//            return 0;
//        }
//        
//        /**
//         * This method will be used to create the record holder heap
//         * 
//         * 
//         */
//        private void createBottomRecordHolderQueue()
//        {
//            holders = new PriorityQueue<TopNHolder>(topN, new Comparator<TopNHolder>()
//            {
//                /**
//                 * compare method to compare to row
//                 * 
//                 * @param r1
//                 *            row 1
//                 * @param r2
//                 *            row 2
//                 * @return compare result
//                 * 
//                 */
//                public int compare(TopNHolder r1, TopNHolder r2)
//                {
//                   return compareBottom(r1, r2);
//                }
//            });
//        }
//        
//    }
//    
//    
//    /**
//     * This class aggregates and holds the rows as per the topN applied on dimension and measure. 
//     * 
//     * @author R00900208
//     *
//     */
//    private static final class TopNHolder 
//    {
//        /**
//         * topNDimIndex
//         */
//        private int topNDimIndex;
//        
//        /**
//         * msrIndex
//         */
//        private int msrIndex;
//        
//        /**
//         * rows list
//         */
//        private List<double[]> rows = new ArrayList<double[]>();
//        
//        /**
//         * lastRow
//         */
//        private double[] lastRow;
//        
//        /**
//         * MeasureAggregator
//         */
//        private MeasureAggregator agg;
//        
//        /**
//         * countMsrIndex
//         */
//        private int countMsrIndex;
//        
//        /**
//         * avgMsrIndex
//         */
//        private int avgMsrIndex;
//        
//    
////        /**
////         * Constructor that takes dimension index and measure index on which topN needs to be applied.
////         * @param dimIndex
////         * @param msrIndex
////         * @param aggName
////         * @param countMsrIndex
////         */
////        private TopNHolder(int dimIndex,int msrIndex,String aggName, int countMsrIndex,int avgMsrIndex)
////        {
////            this.topNDimIndex = dimIndex;
////            this.msrIndex = msrIndex;
////            this.countMsrIndex = countMsrIndex;
////            this.avgMsrIndex = avgMsrIndex;
////            if(avgMsrIndex >= 0)
////            {
////                agg = AggUtil.getAggregator(MolapCommonConstants.AVERAGE, false, null);
////            }
////            else
////            {
////                agg = AggUtil.getAggregator(aggName, false, null);
////            }
////        }
//        
//        /**
//         * Add row to this holder.
//         * @param row
//         * @return, it returns true if it aggregated and belonged to same group.
//         */
//        public boolean addRow(double[] row)
//        {
//            if(lastRow == null)
//            {
//                rows.add(row);
//                lastRow = row;
//                aggregateData(row);
//                return true;
//            }
//            
//            if(objectEquals(lastRow, row))
//            {
//                rows.add(row);
//                lastRow = row;
//                aggregateData(row);
//                return true;
//            }
//            return false;
//        }
//        
//        /**
//         * Aggregate the data
//         * @param row
//         */
//        private void aggregateData(double[] row)
//        {
//            if(avgMsrIndex < 0)
//            {
//                agg.agg(row[msrIndex], null, 0, 0);
//            }
//            else
//            {
//                agg.agg(row[msrIndex],row[countMsrIndex]);
//            }
//        }
//        
//        /**
//         * Equals the array
//         * @param lastRow
//         * @param row
//         * @return
//         */
//        private boolean objectEquals(double[] lastRow,double[] row)
//        {
//            for(int i = 0;i <= topNDimIndex;i++)
//            {
//                if(!(lastRow[i] == row[i]))
//                {
//                    return false;
//                }
//            }
//            return true;
//        }
//
//    }
}

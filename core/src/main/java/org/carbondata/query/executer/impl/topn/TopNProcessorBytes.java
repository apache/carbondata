/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.executer.impl.topn;

import java.util.*;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.groupby.GroupByHolder;
import org.carbondata.query.executer.impl.measure.filter.MeasureFilterProcessor;
import org.carbondata.query.executer.impl.topn.TopNModel.MolapTopNType;
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;

/**
 * This class calculates the topN rows as per the defined parameters.
 */
public class TopNProcessorBytes
        implements DataProcessor//,Callable< Map<ByteArrayWrapper, MeasureAggregator[]>>
{

    /**
     * topMeasureIndex
     */
    private int topMeasureIndex;
    /**
     * topNCount
     */
    private int topNCount;
    /**
     * topNType
     */
    private MolapTopNType topNType;

    /**
     * groupMaskedBytes
     */
    private byte[] groupMaskedBytes;

    /**
     * groups
     */
    private List<TopNHolderGroup> groups =
            new ArrayList<TopNHolderGroup>(MolapCommonConstants.CONSTANT_SIZE_TEN);

    /**
     *
     */
    private TopNHolderGroup group;

    /**
     *
     */
    //    private GroupByHolder holder;

    /**
     * countMsrIndex
     */
    private int countMsrIndex;

    /**
     *
     */
    private DataProcessor processor;

    /**
     *
     */
    //    private boolean isCalculatedMsr;

    /**
     * Query measures
     */
    //    private Measure[] queryMsrs;

    /**
     * Calc function
     */
    //    private MolapCalcFunction calcFunction;

    /**
     * maskedBytesPos
     */
    //    private int[] maskedBytesPos;

    /**
     * maskedBytesPosForGroup
     */
    private int[] maskedBytesPosForGroup;

    //    private List<byte[]> keys = null;

    //    private List<MeasureAggregator[]> aggregators = null;

    /**
     * topnOnColumns
     */
    private boolean topnOnColumns;

    /**
     * @param dimIndexes
     * @param topMeasureIndex
     * @param topNCount
     * @param topNType
     */
    public TopNProcessorBytes(DataProcessor processor) {
        this.processor = processor;

    }

    public TopNProcessorBytes(List<byte[]> keys, List<MeasureAggregator[]> aggregators) {
        //        this.keys = keys;
        //        this.aggregators = aggregators;
    }

    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException {
        this.groupMaskedBytes = model.getGroupMaskedBytes();
        this.topMeasureIndex = model.getTopMeasureIndex();
        this.topNCount = model.getTopNCount();
        this.topNType = model.getTopNType();
        this.countMsrIndex = model.getCountMsrIndex();
        this.topnOnColumns = model.isTopNOnColumn();
        //        this.isCalculatedMsr = model.isTopCountOnCalcMeasure();
        //        this.queryMsrs = model.getQueryMsrs();
        this.maskedBytesPosForGroup = model.getTopNGroupMaskedBytesPos();
        //        this.maskedBytesPos = model.getTopNMaskedBytesPos();
        //
        //        if(isCalculatedMsr)
        //        {
        //            int calcMsrIndex = topMeasureIndex-queryMsrs.length;
        //            calcFunction = MolapCalcExpressionResolverUtil.createCalcExpressions(model.getCalculatedMeasures()[calcMsrIndex].getExp(), Arrays.asList(queryMsrs));
        //        }
        if (countMsrIndex < 0) {
            countMsrIndex = 0;
        }
        if (topMeasureIndex < 0) {
            topMeasureIndex = 0;
        }
        group = new TopNHolderGroup(groupMaskedBytes, topNCount, topNType, maskedBytesPosForGroup,
                topnOnColumns);
        groups.add(group);
        //        holder = new GroupByHolder(maskedBytes, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex,isCalculatedMsr,queryMsrs,calcFunction,maskedBytesPos);
        if (processor != null) {
            processor.initModel(model);
        }
    }

    //    /**
    //     * Add row to processor.
    //     * @param row
    //     * @param aggregators
    //     */
    //    private void addRow(byte[] row,MeasureAggregator[] aggregators)
    //    {
    //        if(!holder.addRow(row,aggregators))
    //        {
    //            if(!group.addHolder(holder))
    //            {
    //                group = new TopNHolderGroup(groupMaskedBytes,topNCount,topNType,maskedBytesPosForGroup);
    //                groups.add(group);
    //                group.addHolder(holder);
    //            }
    //            holder = new GroupByHolder(maskedBytes, topMeasureIndex, aggName, countMsrIndex, avgMsrIndex,isCalculatedMsr,queryMsrs,calcFunction,maskedBytesPos);
    //            holder.addRow(row,aggregators);
    //        }
    //    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) {
        //       addRow(key, measures);
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder) {
        if (!group.addHolder(groupByHolder)) {
            group = new TopNHolderGroup(groupMaskedBytes, topNCount, topNType,
                    maskedBytesPosForGroup, topnOnColumns);
            groups.add(group);
            group.addHolder(groupByHolder);
        }
    }

    @Override
    public void finish() throws MolapPaginationException {

        if (processor instanceof MeasureFilterProcessor) {
            for (TopNHolderGroup holderGroup : groups) {
                AbstractQueue<GroupByHolder> holders = holderGroup.getHolders();

                int size = holders.size();
                List<GroupByHolder> list =
                        new ArrayList<GroupByHolder>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                for (int i = 0; i < size; i++) {
                    list.add(holders.poll());
                }
                for (int i = list.size() - 1; i >= 0; i--) {
                    processor.processGroup(list.get(i));
                }
            }
        } else {
            for (TopNHolderGroup holderGroup : groups) {
                AbstractQueue<GroupByHolder> holders = holderGroup.getHolders();

                int size = holders.size();
                List<GroupByHolder> list =
                        new ArrayList<GroupByHolder>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                for (int i = 0; i < size; i++) {
                    list.add(holders.poll());
                }
                for (int i = list.size() - 1; i >= 0; i--) {
                    List<byte[]> rows = list.get(i).getRows();
                    List<MeasureAggregator[]> msrs = list.get(i).getMsrs();
                    for (int j = 0; j < rows.size(); j++) {
                        processor.processRow(rows.get(j), msrs.get(j));
                    }

                }
            }
        }

        processor.finish();
    }

    /**
     * This class holds group of TopN holders which are belonged same group as per the top n count
     *
     * @author R00900208
     */
    private static final class TopNHolderGroup {
        /**
         * topN
         */
        private int topN;

        /**
         * holders
         */
        private AbstractQueue<GroupByHolder> holders;

        /**
         * dimIndex
         */
        private byte[] groupMaskedBytes;

        /**
         * lastHolder
         */
        private GroupByHolder lastHolder;

        /**
         * topNType
         */
        private MolapTopNType topNType;

        /**
         * maskedBytesPosForGroup
         */
        private int[] maskedBytesPosForGroup;

        /**
         * topnOnColumnsGroup
         */
        private boolean topnOnColumnsGroup;

        /**
         * Constructor that takes the meta information for this object
         *
         * @param dimIndex  , From where it supposed to do group
         * @param topN      , count of rows
         * @param topNType, whether top or bottom.
         */
        private TopNHolderGroup(byte[] groupMaskedBytes, int topN, MolapTopNType topNType,
                int[] maskedBytesPosForGroup, boolean topnOnColumns) {
            this.topN = topN;
            this.groupMaskedBytes = groupMaskedBytes;
            this.topNType = topNType;
            this.maskedBytesPosForGroup = maskedBytesPosForGroup;
            this.topnOnColumnsGroup = topnOnColumns;
            if (topNType.equals(MolapTopNType.TOP)) {
                createTopRecordHolderQueue();
            } else {
                createBottomRecordHolderQueue();
            }
        }

        /**
         * Add the topn holder to this group.
         *
         * @param holder
         * @return, it returns true if it belonged to same group and added to it or else false.
         */
        public boolean addHolder(GroupByHolder holder) {
            if (lastHolder == null) {
                holders.add(holder);
                lastHolder = holder;
                return true;
            }

            if (equalsObject(lastHolder.lastRow, holder.lastRow)) {
                if (holders.size() >= topN) {
                    GroupByHolder peek = holders.peek();
                    handleTopNType(holder, peek);
                } else {

                    holders.add(holder);
                }
                lastHolder = holder;
                return true;
            }
            return false;
        }

        private void handleTopNType(GroupByHolder holder, GroupByHolder peek) {
            if (topNType.equals(MolapTopNType.TOP)) {
                if (null != peek && compareTop(holder, peek) > 0) {
                    holders.poll();
                    holders.add(holder);
                }
            } else {
                if (null != peek && compareBottom(holder, peek) > 0) {
                    holders.poll();
                    holders.add(holder);
                }
            }
        }

        /**
         * Equals check for two arrays.
         *
         * @param lastRow
         * @param row
         * @return
         */
        private boolean equalsObject(byte[] lastRow, byte[] row) {
            int length = maskedBytesPosForGroup.length;
            if (length == 0 && topnOnColumnsGroup) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                byte lb = (byte) (groupMaskedBytes[i] & lastRow[maskedBytesPosForGroup[i]]);
                byte rb = (byte) (groupMaskedBytes[i] & row[maskedBytesPosForGroup[i]]);
                if (lb != rb) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Get all the holders with in this group.
         *
         * @return
         */
        public AbstractQueue<GroupByHolder> getHolders() {
            return holders;
        }

        /**
         * This method will be used to create the record holder heap
         */
        private void createTopRecordHolderQueue() {
            holders = new PriorityQueue<GroupByHolder>(topN, new Comparator<GroupByHolder>() {
                /**
                 * compare method to compare to row
                 *
                 * @param r1
                 *            row 1
                 * @param r2
                 *            row 2
                 * @return compare result
                 *
                 */
                public int compare(GroupByHolder r1, GroupByHolder r2) {
                    return compareTop(r1, r2);
                }
            });
        }

        private int compareTop(GroupByHolder r1, GroupByHolder r2) {
            double v1 = r1.getValue();
            double v2 = r2.getValue();
            if (v1 > v2) {
                return 1;
            }
            if (v1 < v2) {
                return -1;
            }

            return 0;
        }

        /**
         * compare method to compare to row
         *
         * @param r1 row 1
         * @param r2 row 2
         * @return compare result
         */
        private int compareBottom(GroupByHolder r1, GroupByHolder r2) {
            double v1 = r1.getValue();
            double v2 = r2.getValue();
            if (v1 > v2) {
                return -1;
            }
            if (v1 < v2) {
                return 1;
            }

            return 0;
        }

        /**
         * This method will be used to create the record holder heap
         */
        private void createBottomRecordHolderQueue() {
            holders = new PriorityQueue<GroupByHolder>(topN, new Comparator<GroupByHolder>() {
                /**
                 * compare method to compare to row
                 *
                 * @param r1
                 *            row 1
                 * @param r2
                 *            row 2
                 * @return compare result
                 *
                 */
                public int compare(GroupByHolder r1, GroupByHolder r2) {
                    return compareBottom(r1, r2);
                }
            });
        }

    }

    //    @Override
    //    public  Map<ByteArrayWrapper, MeasureAggregator[]> call() throws Exception
    //    {
    //
    //        for(int i = 0;i < keys.size();i++)
    //        {
    //            addRow(keys.get(i), aggregators.get(i));
    //        }
    //
    //        return getData();
    //    }
    //
    //    public Map<ByteArrayWrapper, MeasureAggregator[]> getData() throws MolapPaginationException
    //    {
    //        if(holder.getRows().size() > 0)
    //        {
    //            if(!group.addHolder(holder))
    //            {
    //                group = new TopNHolderGroup(groupMaskedBytes,topNCount,topNType,maskedBytesPosForGroup);
    //                groups.add(group);
    //                group.addHolder(holder);
    //            }
    //        }
    //        Map<ByteArrayWrapper, MeasureAggregator[]> processedData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>();
    //
    //        for(TopNHolderGroup holderGroup : groups)
    //        {
    //            AbstractQueue<GroupByHolder> holders = holderGroup.getHolders();
    //
    //            int size = holders.size();
    //            List<GroupByHolder> list = new ArrayList<GroupByHolder>();
    //            for(int i = 0;i < size;i++)
    //            {
    //                list.add(holders.poll());
    //            }
    //            for(int i = list.size()-1;i >= 0;i--)
    //            {
    //                List<byte[]> rows = list.get(i).getRows();
    //                List<MeasureAggregator[]> msrs = list.get(i).getMsrs();
    //                for(int j = 0;j < rows.size();j++)
    //                {
    //                    ByteArrayWrapper wrapper = new ByteArrayWrapper();
    //                    wrapper.setMaskedKey(rows.get(j));
    //                    processedData.put(wrapper, msrs.get(j));
    //                }
    //
    //            }
    //        }
    //        return processedData;
    //    }

}

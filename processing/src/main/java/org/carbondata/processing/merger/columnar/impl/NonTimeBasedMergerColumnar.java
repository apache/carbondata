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

package org.carbondata.processing.merger.columnar.impl;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.carbondata.core.util.ByteUtil;
import org.carbondata.processing.factreader.CarbonSurrogateTupleHolder;
import org.carbondata.processing.merger.columnar.ColumnarFactFileMerger;
import org.carbondata.processing.merger.columnar.iterator.CarbonDataIterator;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.schema.metadata.CarbonColumnarFactMergerInfo;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;

public class NonTimeBasedMergerColumnar extends ColumnarFactFileMerger {

    /**
     * record holder heap
     */
    private AbstractQueue<CarbonDataIterator<CarbonSurrogateTupleHolder>> recordHolderHeap;

    public NonTimeBasedMergerColumnar(CarbonColumnarFactMergerInfo carbonColumnarFactMergerInfo,
            int currentRestructNumber) {
        super(carbonColumnarFactMergerInfo, currentRestructNumber);
        if (leafTupleIteratorList.size() > 0) {
            recordHolderHeap = new PriorityQueue<CarbonDataIterator<CarbonSurrogateTupleHolder>>(
                    leafTupleIteratorList.size(), new CarbonMdkeyComparator());
        }
    }

    @Override
    public void mergerSlice() throws SliceMergerException {
        // index
        int index = 0;
        try {
            dataHandler.initialise();
            // add first record from each file
            for (CarbonDataIterator<CarbonSurrogateTupleHolder> leaftTupleIterator : this.leafTupleIteratorList) {
                this.recordHolderHeap.add(leaftTupleIterator);
                index++;
            }
            CarbonDataIterator<CarbonSurrogateTupleHolder> poll = null;
            while (index > 1) {
                // poll the top record
                poll = this.recordHolderHeap.poll();
                // get the mdkey
                addRow(poll.getNextData());
                // if there is no record in the leaf and all then decrement the
                // index
                if (!poll.hasNext()) {
                    index--;
                    continue;
                }
                poll.fetchNextData();
                // add record to heap
                this.recordHolderHeap.add(poll);
            }
            // if record holder is not empty then poll the slice holder from
            // heap
            poll = this.recordHolderHeap.poll();
            while (true) {
                addRow(poll.getNextData());
                // check if leaf contains no record
                if (!poll.hasNext()) {
                    break;
                }
                poll.fetchNextData();
            }
            this.dataHandler.finish();

        } catch (CarbonDataWriterException e) {
            throw new SliceMergerException(
                    "Problem while getting the file channel for Destination file: ", e);
        } finally {
            this.dataHandler.closeHandler();
        }
    }

    private class CarbonMdkeyComparator
            implements Comparator<CarbonDataIterator<CarbonSurrogateTupleHolder>> {

        @Override
        public int compare(CarbonDataIterator<CarbonSurrogateTupleHolder> o1,
                CarbonDataIterator<CarbonSurrogateTupleHolder> o2) {
            return ByteUtil.UnsafeComparer.INSTANCE
                    .compareTo(o1.getNextData().getMdKey(), o2.getNextData().getMdKey());
        }

    }
}

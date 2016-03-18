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

package com.huawei.unibi.molap.merger.columnar.impl;

import com.huawei.unibi.molap.factreader.MolapSurrogateTupleHolder;
import com.huawei.unibi.molap.merger.columnar.ColumnarFactFileMerger;
import com.huawei.unibi.molap.merger.columnar.iterator.MolapDataIterator;
import com.huawei.unibi.molap.merger.exeception.SliceMergerException;
import com.huawei.unibi.molap.schema.metadata.MolapColumnarFactMergerInfo;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;

public class TimeBasedMergerColumnar extends ColumnarFactFileMerger
{

    public TimeBasedMergerColumnar(
            MolapColumnarFactMergerInfo molapColumnarFactMergerInfo, int currentRestructNumber)
    {
        super(molapColumnarFactMergerInfo, currentRestructNumber);
    }

    @Override
    public void mergerSlice() throws SliceMergerException
    {
        try
        {
            dataHandler.initialise();
            for(MolapDataIterator<MolapSurrogateTupleHolder> leaftTupleIterator :leafTupleIteratorList)
            {
                while(true)
                {
                    addRow(leaftTupleIterator.getNextData());
                    if(!leaftTupleIterator.hasNext())
                    {
                        break;
                    }
                    leaftTupleIterator.fetchNextData();
                }
            }
            this.dataHandler.finish();
        }
        catch(MolapDataWriterException e)
        {
            throw new SliceMergerException(
                    "Problem while getting the file channel for Destination file: ",
                    e);
        }
        finally
        {
            this.dataHandler.closeHandler();
        }

    }

}

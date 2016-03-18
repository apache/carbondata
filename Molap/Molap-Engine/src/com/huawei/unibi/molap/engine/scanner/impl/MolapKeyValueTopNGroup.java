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

/**
 * 
 */
package com.huawei.unibi.molap.engine.scanner.impl;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.impl.topn.TopNModel.MolapTopNType;

/**
 * @author R00900208
 *
 */
public class MolapKeyValueTopNGroup extends MolapValue
{
    
    /**
     * 
     */
    private static final long serialVersionUID = 1083284293232310147L;
    
    private AbstractQueue<MolapKeyValueGroup> holders;
    
    private int topN;
    
    private MolapTopNType type;
    

    public MolapKeyValueTopNGroup(MeasureAggregator[] values,MolapValue group,int topN,MolapTopNType type)
    {
        super(values);
        this.topN = topN;
        this.type = type;
        if(type.equals(MolapTopNType.TOP))
        {
            holders = new PriorityQueue<MolapKeyValueGroup>(topN, new TopComparator());
        }
        else
        {
            holders = new PriorityQueue<MolapKeyValueGroup>(topN, new BottomComparator());
        }
        if(group instanceof MolapKeyValueGroup)
        {
            addValue((MolapKeyValueGroup)group);
        }
    }
    
    @Override
    public MolapValue mergeKeyVal(MolapValue another)
    {
        MolapKeyValueTopNGroup group = (MolapKeyValueTopNGroup)another;
        Iterator<MolapKeyValueGroup> iterator = group.holders.iterator();
        while(iterator.hasNext())
        {
            addValue(iterator.next());
        }

        return this;
    }
    
    private void addValue(MolapKeyValueGroup group)
    {
        if(holders.size() >= topN)
        {
            MolapKeyValueGroup peek = holders.peek();
            handleTopNType(group, peek);
        }
        else
        {
            
            holders.add(group);
        }
    }
    
    private void handleTopNType(MolapKeyValueGroup holder, MolapKeyValueGroup peek)
    {
        if(type.equals(MolapTopNType.TOP))
        {
            if(compareTop(holder, peek) > 0)
            {
                holders.poll();
                holders.add(holder);
            }
        }
        else
        {
            if(compareBottom(holder, peek) > 0)
            {
                holders.poll();
                holders.add(holder);
            }
        }
    }
    

    
    private int compareTop(MolapKeyValueGroup r1, MolapKeyValueGroup r2)
    {
        Number left = (Number)r1.getValues()[0].getValue();
        Number right = (Number)r2.getValues()[0].getValue();
        if(left.doubleValue() > right.doubleValue())
        {
            return 1;
        }
        if(left.doubleValue() < right.doubleValue())
        {
            return -1;
        }
        
        return 0;
    }
    
    @Override
    public String toString()
    {
        Iterator<MolapKeyValueGroup> iter = getGroups().iterator();
        StringBuffer bf = new StringBuffer();
        while(iter.hasNext())
        {
            MolapKeyValueGroup next = iter.next();
            List<MolapKey> keys = next.getKeys();
            List<MolapValue> values = next.getAllValues();
            bf.append(keys.toString()).append("      ").append(values.toString());
            bf.append("********");
        }
        return bf.toString();
    }
    
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
    private int compareBottom(MolapKeyValueGroup r1, MolapKeyValueGroup r2)
    {
        Number left = (Number)r1.getValues()[0].getValue();
        Number right = (Number)r2.getValues()[0].getValue();
        if(left.doubleValue() > right.doubleValue())
        {
            return -1;
        }
        if(left.doubleValue() < right.doubleValue())
        {
            return 1;
        }
        
        return 0;
    }
    

    
    public AbstractQueue<MolapKeyValueGroup> getGroups()
    {
        return holders;
    }
    
    private class TopComparator implements Comparator<MolapKeyValueGroup>,Serializable
    {
        /**
         * 
         */
        private static final long serialVersionUID = 5584875770898087076L;

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
        public int compare(MolapKeyValueGroup r1, MolapKeyValueGroup r2)
        {
            return compareTop(r1, r2);
        }
    }
    
    private class BottomComparator implements Comparator<MolapKeyValueGroup>,Serializable
    {


        /**
         * 
         */
        private static final long serialVersionUID = 263377325485307791L;

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
        public int compare(MolapKeyValueGroup r1, MolapKeyValueGroup r2)
        {
           return compareBottom(r1, r2);
        }
    }
    
//    private static class Tuple
//    {
//        private HbaseKey key;
//        
//        private HbaseKeyValueGroup value;
//        
//    }
}

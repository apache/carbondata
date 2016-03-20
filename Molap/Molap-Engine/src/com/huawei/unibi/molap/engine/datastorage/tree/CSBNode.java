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

package com.huawei.unibi.molap.engine.datastorage.tree;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;

public abstract class CSBNode implements DataStoreBlock
{
    public abstract boolean isLeafNode();

    public abstract double[] getValue(int keyindex);

    public abstract CSBNode getChild(int childIndex);

    public abstract int getnKeys();

    public abstract CSBNode getNext();

    public abstract void setChildren(CSBNode[] children);

    public abstract void setNextNode(CSBNode nextNode);

    public abstract void setNext(CSBNode nextNode);

    public abstract void setKey(int keyindex, byte[] key);
    
    public abstract byte[] getKey(int keyIndex, FileHolder fileHolder);
    
    public abstract void addEntry(Pair<byte[], double[]> entry);
}

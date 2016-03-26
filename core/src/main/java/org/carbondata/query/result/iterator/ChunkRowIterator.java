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

package org.carbondata.query.result.iterator;

import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.query.result.ChunkResult;
import org.carbondata.query.result.RowResult;

public class ChunkRowIterator implements MolapIterator<RowResult> {
    private MolapIterator<ChunkResult> iterator;

    private ChunkResult currentchunk;

    public ChunkRowIterator(MolapIterator<ChunkResult> iterator) {
        this.iterator = iterator;
        if (iterator.hasNext()) {
            currentchunk = iterator.next();
        }
    }

    @Override public boolean hasNext() {
        if (null != currentchunk) {
            if ((currentchunk.hasNext())) {
                return true;
            } else if (!currentchunk.hasNext()) {
                while (iterator.hasNext()) {
                    currentchunk = iterator.next();
                    if (currentchunk != null && currentchunk.hasNext()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override public RowResult next() {
        return currentchunk.next();
    }

}

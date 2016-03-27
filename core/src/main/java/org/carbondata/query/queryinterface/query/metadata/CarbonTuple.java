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

package org.carbondata.query.queryinterface.query.metadata;

import java.io.Serializable;
import java.util.Arrays;

/**
 * MolapTuple class , it contains the each row or column information of query result.
 */
public class CarbonTuple implements Serializable {
    private static final long serialVersionUID = 6432454407461679716L;

    private CarbonMember[] tuple;

    public CarbonTuple(CarbonMember[] tuple) {
        this.tuple = tuple;
    }

    /**
     * Size of tuple.
     *
     * @return
     */
    public int size() {
        return tuple.length;
    }

    /**
     * Get all members inside tuple.
     *
     * @return
     */
    public CarbonMember[] getTuple() {
        return tuple;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(tuple);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CarbonTuple) {
            if (this == obj) {
                return true;
            }
            CarbonTuple other = (CarbonTuple) obj;
            if (!Arrays.equals(tuple, other.tuple)) {
                return false;
            }
            return true;

        }
        return false;
    }
}

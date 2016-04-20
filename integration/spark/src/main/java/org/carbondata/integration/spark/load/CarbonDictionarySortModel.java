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
package org.carbondata.integration.spark.load;

import org.carbondata.core.util.ByteUtil;

/**
 * Dictionary sort model class holds the member byte value and corresponding key value.
 */
public class CarbonDictionarySortModel implements Comparable<CarbonDictionarySortModel> {

    /**
     * Surrogate key
     */
    private int key;

    /**
     * member value in bytes
     */
    private byte[] memberBytes;

    /**
     * CarbonDictionarySortModel
     *
     * @param key
     * @param memberBytes
     */
    public CarbonDictionarySortModel(int key, byte[] memberBytes) {
        this.key = key;
        this.memberBytes = memberBytes;
    }

    /**
     * Compare
     */
    @Override public int compareTo(CarbonDictionarySortModel o) {

        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(this.memberBytes, o.memberBytes);
    }

    /**
     * @see Object#hashCode()
     */
    @Override public int hashCode() {
        int result = 1;
        result = result + ((memberBytes == null) ? 0 : memberBytes.hashCode());
        return result;
    }

    /**
     * @see Object#equals(Object)
     */
    @Override public boolean equals(Object obj) {
        if (obj instanceof CarbonDictionarySortModel) {
            if (this == obj) {
                return true;
            }
            CarbonDictionarySortModel other = (CarbonDictionarySortModel) obj;
            if (memberBytes == null) {
                if (other.memberBytes != null) {
                    return false;
                }
            } else if (!ByteUtil.UnsafeComparer.INSTANCE
                    .equals(this.memberBytes, other.memberBytes)) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * return the surrogate of the member
     *
     * @return
     */
    public int getKey() {
        return key;
    }

    /**
     * Returns member buye
     *
     * @return
     */
    public byte[] getMemberBytes() {
        return memberBytes;
    }
}

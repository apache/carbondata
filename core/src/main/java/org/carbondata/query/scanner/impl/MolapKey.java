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
package org.carbondata.query.scanner.impl;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author R00900208
 */
public class MolapKey implements Serializable, Comparable<MolapKey> {

    /**
     *
     */
    private static final long serialVersionUID = -8773813519739848506L;

    private Object[] key;

    public MolapKey(Object[] key) {
        this.key = key;
    }

    /**
     * @return the key
     */
    public Object[] getKey() {
        return key;
    }

    public MolapKey getSubKey(int size) {
        Object[] crop = new Object[size];
        System.arraycopy(key, 0, crop, 0, size);
        return new MolapKey(crop);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(key);
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MolapKey other = (MolapKey) obj;
        if (!Arrays.equals(key, other.key)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(key);
    }

    @Override
    public int compareTo(MolapKey other) {
        Object[] oKey = other.key;

        int l = 0;
        for (int i = 0; i < key.length; i++) {
            l = ((Comparable) key[i]).compareTo(oKey[i]);
            if (l != 0) {
                return l;
            }
        }

        return 0;
    }

}

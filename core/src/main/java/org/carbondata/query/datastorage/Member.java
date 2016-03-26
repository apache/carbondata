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

package org.carbondata.query.datastorage;

import java.util.Arrays;

import org.carbondata.core.util.ByteUtil;

public class Member {

    /**
     * All the attributes related to member. ordinalCOlumn, caption column,
     * names column and properties. Order is same as how SQLMemberSource reads
     * the columns. Current Store: ordinalCOlumn, Properties.
     */
    protected Object[] attributes;

    /**
     *
     */
    protected byte[] name;

    public Member(final byte[] name) {
        this.name = name;
    }

    /**
     * @return the name
     */
    public byte[] getCharArray() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Member) {
            if (this == obj) {
                return true;
            }
            //            return Arrays.equals(name, ((Member)obj).name);
            return ByteUtil.UnsafeComparer.INSTANCE.equals(name, ((Member) obj).name);
        } else {
            return false;
        }
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(name);
        return result;
    }

    @Override
    public String toString() {
        // return str;
        return new String(name);
    }

    /**
     * @return
     */
    public Object[] getAttributes() {
        return attributes;
    }

    /**
     * @param properties
     */
    public void setAttributes(final Object[] properties) {
        this.attributes = properties;
    }

}

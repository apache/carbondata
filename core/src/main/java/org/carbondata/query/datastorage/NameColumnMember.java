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

/**
 * Member with name column. If ROLAP level is having name column, comparison
 * should happen on name column not on key column.
 *
 * @author K00900207
 */
public class NameColumnMember {
    //    /**
    //     *
    //     */
    //    private int nameColumnIndex;
    //
    //    public NameColumnMember(byte[] name, int nameColIndex)
    //    {
    //        super(name);
    //        this.nameColumnIndex = nameColIndex;
    //    }
    //
    //    /**
    //     *
    //     * @see com.huawei.unibi.molap.engine.datastorage.Member#equals(java.lang.Object)
    //     */
    //    public boolean equals(Object obj)
    //    {
    //        if(obj instanceof NameColumnMember)
    //        {
    //            if(this == obj)
    //            {
    //                return true;
    //            }
    //
    //            return attributes[nameColumnIndex].equals(((NameColumnMember)obj).attributes[nameColumnIndex]);
    //        }
    //        else
    //        {
    //            if((obj instanceof Member))
    //            {
    //                return super.equals(obj);
    //            }
    //            else
    //            {
    //                return false;
    //            }
    //        }
    //
    //    }
    //
    //
    //    /**
    //     *
    //     * @see java.lang.Object#hashCode()
    //     *
    //     */
    //    @Override
    //    public int hashCode()
    //    {
    //        final int prime = 31;
    //        int result = 1;
    //        result = prime * result + attributes[nameColumnIndex].hashCode();
    //        return result;
    //    }
    //
    //    /*
    //     * @Override public int compareTo(Member o) { char v1[] =
    //     * ((String)attributes[nameColumnIndex]).toCharArray(); char v2[] =
    //     * ((String)o.attributes[nameColumnIndex]).toCharArray();; int len1 =
    //     * v1.length; int len2 = v2.length; int n = Math.min(len1, len2);
    //     *
    //     * int k = 0; while (k <n) { char c1 = v1[k]; char c2 = v2[k]; if (c1 != c2)
    //     * { return c1 - c2; } k++; }
    //     *
    //     * return len1 - len2; }
    //     */

}

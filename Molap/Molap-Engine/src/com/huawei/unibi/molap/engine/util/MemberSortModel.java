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

package com.huawei.unibi.molap.engine.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.datastorage.DataType;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * Below class will be used to create the sort model which will be used to sort the data 
 * @author K00900841
 *
 */
public class MemberSortModel implements Comparable<MemberSortModel>
{
    /**
     * Surrogate key
     */
    private int key;

    /**
     * memberName
     */
    private String memberName;

    private byte[] memberBytes;
    
    private DataType memberDataType;

    /**
     * @param key
     * @param member
     */
    public MemberSortModel(int key, String member,
            byte[] memberBytes, DataType memberDataType)
    {
        this.key = key;
        // this.member = member;
        memberName = member;
        this.memberBytes = memberBytes;
        this.memberDataType=memberDataType;
    }

    /**
     * Compare
     */
    @Override
    public int compareTo(MemberSortModel o)
    {
        switch(memberDataType)
        {
        case NUMBER:
            Double d1 = null;
            Double d2 = null;
            try
            {
                d1 = new Double(memberName);
            }
            catch(NumberFormatException e)
            {
                return -1;
            }
            try
            {
                d2 = new Double(o.memberName);
            }
            catch(NumberFormatException e)
            {
                return 1;
            }

            return d1.compareTo(d2);
        case TIMESTAMP:
            SimpleDateFormat timeParser = new SimpleDateFormat(
                    MolapProperties
                            .getInstance()
                            .getProperty(
                                    MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT,
                                    MolapCommonConstants.MOLAP_TIMESTAMP_DEFAULT_FORMAT));
            Date date1 = null;
            Date date2 = null;
            try
            {
                date1 = timeParser.parse(memberName);
            }
            catch(ParseException e)
            {
                return -1;
            }
            try
            {
                date2 = timeParser.parse(o.memberName);
            }
            catch(ParseException e)
            {
                return 1;
            }
            return (int)(date1.getTime() - date2.getTime());
        case STRING:
        default:
            return ByteUtil.UnsafeComparer.INSTANCE.compareTo(
                    this.memberBytes, o.memberBytes);
        }
    }

    /**
     * 
     * @see java.lang.Object#hashCode()
     * 
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((memberName == null) ? 0 : memberName.hashCode());
        return result;
    }

    /**
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     * 
     */
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof MemberSortModel)
        {
            if(this == obj)
            {
                return true;
            }
            MemberSortModel other = (MemberSortModel)obj;
            if(memberName == null)
            {
                if(other.memberName != null)
                {
                    return false;
                }
            }
            else if(!memberName.equals(other.memberName))
            {
                return false;
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    public int getKey()
    {
        return key;
    }

    public String getMemberName()
    {
        return memberName;
    }

    public byte[] getMemberBytes()
    {
        return memberBytes;
    }
}
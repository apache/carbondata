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

package org.carbondata.query.filters.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.query.filters.likefilters.FilterLikeExpressionIntf;
import org.carbondata.query.queryinterface.filter.MolapFilterInfo;

/**
 * This class is used for content match of level string.
 */
public class ContentMatchFilterInfo extends MolapFilterInfo {
    /**
     * excludedContentMatchMembers.
     */
    private List<String> excludedContentMatchMembers;
    /**
     * includedContentMatchMembers.
     */
    private List<String> includedContentMatchMembers;

    /**
     * dimFilterMap
     */
    private Map<Integer, ContentMatchFilterInfo> dimFilterMap =
            new HashMap<Integer, ContentMatchFilterInfo>(16);
    /**
     * Include like filters.
     * Ex: select employee_name,department_name,sum(salary) from employee where employee_name like ("a","b");
     * then "a" and "b" will be the include like filters.
     */
    private List<FilterLikeExpressionIntf> likeFilterExpressions;

    /**
     * @return the excludedContentMatchMembers
     */
    public List<String> getExcludedContentMatchMembers() {
        return excludedContentMatchMembers;
    }

    /**
     * @param excludedContentMatchMembers the excludedContentMatchMembers to set
     */
    public void setExcludedContentMatchMembers(List<String> excludedContentMatchMembers) {
        this.excludedContentMatchMembers = excludedContentMatchMembers;
    }

    /**
     * @return the includedContentMatchMembers
     */
    public List<String> getIncludedContentMatchMembers() {
        return includedContentMatchMembers;
    }

    /**
     * @param includedContentMatchMembers the includedContentMatchMembers to set
     */
    public void setIncludedContentMatchMembers(List<String> includedContentMatchMembers) {
        this.includedContentMatchMembers = includedContentMatchMembers;
    }

    /**
     * @return the excludeLikeFilter
     */
    public List<FilterLikeExpressionIntf> getLikeFilterExpression() {
        return likeFilterExpressions;
    }

    /**
     * @param includeLikeFilter the includeLikeFilter to set
     */
    public void setLikeFilterExpression(
            List<FilterLikeExpressionIntf> listOfFilterLikeExpressionIntf) {
        this.likeFilterExpressions = listOfFilterLikeExpressionIntf;
    }

    /**
     * hashCode
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int res = 1;
        res = prime * res + ((excludedMembers == null) ? 0 : excludedMembers.hashCode());
        res = prime * res + ((includedMembers == null) ? 0 : includedMembers.hashCode());
        return res;
    }

    /**
     * equals
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ContentMatchFilterInfo) {

            if (this == obj) {
                return true;
            }

            ContentMatchFilterInfo other = (ContentMatchFilterInfo) obj;

            if (excludedMembers == null) {
                if (other.excludedMembers != null) {
                    return false;
                }
            } else if (!excludedMembers.equals(other.excludedMembers)) {
                return false;
            }
            if (includedMembers == null) {
                if (other.includedMembers != null) {
                    return false;
                }
            } else if (!includedMembers.equals(other.includedMembers)) {
                return false;
            }
            return true;
        }

        return false;
    }

    /**
     * @return the dimFilterMap
     */
    public Map<Integer, ContentMatchFilterInfo> getDimFilterMap() {
        return dimFilterMap;
    }

    /**
     * @param dimFilterMap the dimFilterMap to set
     */
    public void setDimFilterMap(Map<Integer, ContentMatchFilterInfo> dimFilterMap) {
        this.dimFilterMap = dimFilterMap;
    }

}

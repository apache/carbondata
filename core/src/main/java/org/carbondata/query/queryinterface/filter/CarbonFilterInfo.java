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

package org.carbondata.query.queryinterface.filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CarbonFilterInfo implements Serializable {
    private static final long serialVersionUID = -6835223191506253050L;

    protected List<String> excludedMembers;

    protected List<String> includedMembers;

    /**
     * includedMembers or .
     */
    protected List<String> includedOrMembers;

    /**
     * CarbonFilterInfo.
     */
    public CarbonFilterInfo() {
        super();
        this.excludedMembers = new ArrayList<String>(10);
        this.includedMembers = new ArrayList<String>(10);
        this.includedOrMembers = new ArrayList<String>(10);
    }

    /**
     * CarbonFilterInfo.
     *
     * @param exludedMembers
     * @param includedMembers
     */
    public CarbonFilterInfo(List<String> exludedMembers, List<String> includedMembers) {
        super();
        this.excludedMembers =
                (null == exludedMembers ? new ArrayList<String>(10) : exludedMembers);
        this.includedMembers =
                (null == includedMembers ? new ArrayList<String>(10) : includedMembers);
        this.includedOrMembers = new ArrayList<String>(10);
    }

    /**
     * getExcludedMembers.
     *
     * @return List<String>.
     */
    public List<String> getExcludedMembers() {
        return excludedMembers;
    }

    /**
     * getIncludedMembers.
     *
     * @return List<String>.
     */
    public List<String> getIncludedMembers() {
        return includedMembers;
    }

    /**
     * addIncludedMembers.
     *
     * @param aMember
     */
    public void addIncludedMembers(String aMember) {
        includedMembers.add(aMember);
    }

    /**
     * addExcludedMembers.
     *
     * @param aMember
     */
    public void addExcludedMembers(String aMember) {
        excludedMembers.add(aMember);
    }

    /**
     * addIncludedMembers.
     *
     * @param aMember
     */
    public void addAllIncludedMembers(List<String> members) {
        includedMembers.addAll(members);
    }

    /**
     * addExcludedMembers.
     *
     * @param aMember
     */
    public void addAllExcludedMembers(List<String> members) {
        excludedMembers.addAll(members);
    }

    /**
     * Final filter is a intersection
     *
     * @return List<String>.
     */
    public List<String> getEffectiveIncludedMembers() {

        List<String> effectiveMems = new ArrayList<String>(includedMembers);
        effectiveMems.removeAll(excludedMembers);
        return effectiveMems;
    }

    /**
     * getEffectiveExcludedMembers
     *
     * @return List<String>.
     */
    public List<String> getEffectiveExcludedMembers() {
        return includedMembers.size() > 0 ? new ArrayList<String>(10) : excludedMembers;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((excludedMembers == null) ? 0 : excludedMembers.hashCode());
        result = prime * result + ((includedMembers == null) ? 0 : includedMembers.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CarbonFilterInfo) {

            if (this == obj) {
                return true;
            }

            CarbonFilterInfo info = (CarbonFilterInfo) obj;

            if (excludedMembers == null) {
                if (info.excludedMembers != null) {
                    return false;
                }
            } else if (!excludedMembers.equals(info.excludedMembers)) {
                return false;
            }
            if (includedMembers == null) {
                if (info.includedMembers != null) {
                    return false;
                }
            } else if (!includedMembers.equals(info.includedMembers)) {
                return false;
            }
            return true;
        }

        return false;
    }

    /**
     * @return the includedOrMembers
     */
    public List<String> getIncludedOrMembers() {
        return includedOrMembers;
    }
}

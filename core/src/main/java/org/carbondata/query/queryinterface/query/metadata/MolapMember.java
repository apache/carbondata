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

/**
 * It is the Member object which holds information of each member which contained in query result.
 */
public class MolapMember implements Serializable {
    private static final long serialVersionUID = 2149598237303284053L;

    private Object name;

    private Object[] properties;

    /**
     * Constructor that takes filter information for each member.
     *
     * @param name
     * @param properties
     */
    public MolapMember(Object name, Object[] properties) {
        this.name = name;
        this.properties = properties;
    }

    /**
     * @return the name
     */
    public Object getName() {
        return name;
    }

    /**
     * @return the properties
     */
    public Object[] getProperties() {
        return properties;
    }

    /**
     * @return the properties
     */
    public void setProperties(Object[] props) {
        this.properties = props;
    }

    @Override
    public String toString() {
        return name != null ? name.toString() : "";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MolapMember) {
            if (this == obj) {
                return true;
            }

            MolapMember other = (MolapMember) obj;
            if (!(name == null ? other.name == null : name.equals(other.name))) {
                return false;
            }
            return true;

        }
        return false;
    }
}

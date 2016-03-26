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

/**
 * Calculated measures can be created by using this class
 */
public class MolapCalculatedMeasure extends MolapMeasure {
    private static final long serialVersionUID = 4176313704077360543L;
    private String expression;
    private boolean groupCount;
    private String groupDimensionFormula;
    private MolapDimensionLevel groupDimensionLevel;

    public MolapCalculatedMeasure(String measureName, String expr) {
        super(measureName);
    }

    /**
     * @return the expression
     */
    public String getExpression() {
        return expression;
    }

    /**
     * @param expression the expression to set
     */
    public void setExpression(String expression) {
        this.expression = expression;
    }

    /**
     * @return Returns the groupCount.
     */
    public boolean isGroupCount() {
        return groupCount;
    }

    /**
     * @param groupCount The groupCount to set.
     */
    public void setGroupCount(boolean groupCount) {
        this.groupCount = groupCount;
    }

    /**
     * @return Returns the groupDimensionFormula.
     */
    public String getGroupDimensionFormula() {
        return groupDimensionFormula;
    }

    /**
     * @param groupDimensionFormula The groupDimensionFormula to set.
     */
    public void setGroupDimensionFormula(String groupDimensionFormula) {
        this.groupDimensionFormula = groupDimensionFormula;
    }

    /**
     * @return Returns the groupDimensionLevel.
     */
    public MolapDimensionLevel getGroupDimensionLevel() {
        return groupDimensionLevel;
    }

    /**
     * @param groupDimensionLevel The groupDimensionLevel to set.
     */
    public void setGroupDimensionLevel(MolapDimensionLevel groupDimensionLevel) {
        this.groupDimensionLevel = groupDimensionLevel;
    }

    /**
     * See interface comments
     */
    @Override public MolapLevelType getType() {
        return MolapLevelType.CALCULATED_MEASURE;
    }

}

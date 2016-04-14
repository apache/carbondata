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

package org.carbondata.processing.suggest.datastats.util;

import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.core.carbon.CarbonDef.Measure;
import org.carbondata.processing.suggest.autoagg.model.AggSuggestion;
import org.carbondata.processing.suggest.autoagg.model.Request;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.query.querystats.Preference;

public final class AggCombinationGeneratorUtil {

    private AggCombinationGeneratorUtil() {

    }

    /**
     * It calculate maximum possible rows of given combination
     *
     * @param aggCombinationLevels
     * @return
     */
    public static BigInteger getMaxPossibleRows(Level[] aggCombinationLevels) {
        // It will be sorted based on cardinality in descending order
        Arrays.sort(aggCombinationLevels);

        Level NoDictionaryLevel = aggCombinationLevels[0];

        int noDictionary = NoDictionaryLevel.getCardinality();
        BigInteger rows = BigInteger.valueOf(noDictionary);
        for (int i = 1; i < aggCombinationLevels.length; i++) {
            int cardinality = aggCombinationLevels[i].getCardinality();
            int distinct = cardinality;
            if (cardinality > Preference.IGNORE_CARDINALITY) {
                distinct = getDistinctData(aggCombinationLevels, aggCombinationLevels[i], i);
            }
            rows = rows.multiply(BigInteger.valueOf(distinct));
        }
        return rows;
    }

    private static int getDistinctData(Level[] aggCombinationLevels, Level level,
            int currentIndex) {
        int minValue = 1;

        for (int i = 0; i < currentIndex; i++) {
            Level aggCombinationLevel = aggCombinationLevels[i];

            Map<Integer, Integer> distinctRel = aggCombinationLevel.getOtherDimesnionDistinctData();
            int val = distinctRel.get(level.getOrdinal());
            if (i == 0) {
                minValue = val;
            }
            if (val < minValue) {
                minValue = val;
            }
        }

        return minValue;
    }

    /**
     * Generate all possible combination of given aggregate dimensions
     *
     * @param levels
     * @param aggCombinations
     * @param noOfMeasures
     * @param maxPossibleRows
     * @param benefitRatio
     */
    public static List<AggSuggestion> generateCombination(List<Level> levels,
            BigInteger maxPossibleRows, int benefitRatio) {
        List<AggSuggestion> aggCombinations =
                new ArrayList<AggSuggestion>(Preference.AGG_COMBINATION_SIZE);
        // get the number of elements in the set
        int elements = levels.size();

        // the number of members of a power set is 2^n
        Double doubleVal = Math.pow(2, elements);
        long powerElements = doubleVal.longValue();
        // run a binary counter for the number of power elements

        long unnoticed = 0;
        for (long i = powerElements - 1; i >= 0; i--) {
            if (aggCombinations.size() == Preference.AGG_COMBINATION_SIZE) {
                break;
            }

            // convert the binary number to a string containing n digits
            String binary = AggCombinationGeneratorUtil.intToBinary(i, elements);
            // System.out.println(binary);
            // create a new set
            List<Level> innerSet = new ArrayList<Level>(levels.size());

            // convert each digit in the current binary number to the
            // corresponding element
            // in the given set
            for (int j = 0; j < binary.length(); j++) {
                if (binary.charAt(j) == '1') {
                    innerSet.add(levels.get(j));
                }

            }

            // add the new set to the power set
            if (innerSet.size() > 0) {
                AggSuggestion combination =
                        new AggSuggestion(innerSet.toArray(new Level[innerSet.size()]),
                                Request.DATA_STATS);
                int calculatedBenefitRatio =
                        maxPossibleRows.divide(combination.getPossibleRows()).intValue();
                if (calculatedBenefitRatio >= benefitRatio) {
                    aggCombinations.add(combination);
                    // if current combination is 1000110111
                    // than it will 2(^3)-1 subsets.. 3 is nothing but last 1's
                    long skipIndex = getSubsetIndexesToSkip(binary);
                    i = i - skipIndex;
                    unnoticed = 0;
                } else {
                    unnoticed++;
                }
                if (unnoticed >= 100) {
                    // if 11110010101001 is not giving any combination than take
                    // a long jump by subtracting last digit
                    // i.e 01000000000000
                    long skipIndex = getLongJump(binary);
                    i = i - skipIndex;
                    unnoticed = 0;
                }
            }

        }
        return aggCombinations;

    }

    /**
     * if binary no is 1111010 than this return 0100000
     *
     * @param binary
     * @return
     */
    private static long getLongJump(String binary) {
        StringBuffer buf = new StringBuffer();
        boolean foundOne = false;
        for (int i = 0; i < binary.length(); i++) {
            if (foundOne) {
                buf.append('0');
            } else if (binary.charAt(i) == '1') {
                buf.append('0');
                buf.append('1');
                i++;

                foundOne = true;
            }

        }
        return Long.parseLong(buf.toString(), 2);
    }

    /**
     * if binary no is 1011 than it returns (3) last 11 value
     *
     * @param binary
     * @return
     */
    private static long getSubsetIndexesToSkip(String binary) {
        StringBuffer buf = new StringBuffer();
        //		String empty=new String("");
        for (int i = binary.length() - 1; i >= 0; i--) {
            if (binary.charAt(i) == '1') {
                buf.append(binary.charAt(i));
            } else {
                break;
            }
        }
        if (buf.toString().equals(StringUtils.EMPTY)) {
            return 0;
        }
        return Long.parseLong(buf.toString(), 2);
    }

    public static String intToBinary(long binary, int digits) {

        String temp = Long.toBinaryString(binary);
        int foundDigits = temp.length();
        String returner = temp;
        for (int i = foundDigits; i < digits; i++) {
            returner = '0' + returner;
        }

        return returner;
    }

    /**
     * Create aggregate script for all combination
     *
     * @param aggSuggest
     * @param cube
     * @param string
     * @return
     */
    public static List<String> createAggregateScript(List<AggSuggestion> aggSuggest, Cube cube,
            String schemaName, String cubeName) {
        List<String> aggSuggestScripts = new ArrayList<String>(aggSuggest.size());
        for (AggSuggestion combination : aggSuggest) {
            Level[] levels = combination.getAggLevels();
            StringBuffer dims = new StringBuffer();
            for (int i = 0; i < levels.length; i++) {
                dims.append(levels[i].getName());
                if (i < levels.length - 1) {
                    dims.append(",");
                }
            }

            String measureStr = getMeasures(cube);
            //when measure is there than append , at end
            //e.g create aggregate level1,level2,msr1,msr2
            if (measureStr.length() > 0 && dims.length() > 0) {
                dims.append(",");
            }
            String formattedCubeName = cubeName;
            if (!"default".equalsIgnoreCase(schemaName)) {
                formattedCubeName = schemaName + "." + cubeName;
            }
            String script = MessageFormat
                    .format(Preference.AGGREGATE_TABLE_SCRIPT, dims.toString(), measureStr,
                            formattedCubeName);

            aggSuggestScripts.add(script);

        }
        return aggSuggestScripts;
    }

    /**
     * this method will give levels which are enabled
     *
     * @param aggLevels
     * @param cube
     * @return
     */
    public static Level[] getVisibleLevels(Level[] aggLevels, Cube cube) {
        List<Level> visibleLevels = new ArrayList<Level>();
        for (int i = 0; i < aggLevels.length; i++) {
            if (isVisible(aggLevels[i], cube)) {
                visibleLevels.add(aggLevels[i]);
            }
        }
        return visibleLevels.toArray(new Level[visibleLevels.size()]);
    }

    public static boolean isVisible(Level level, Cube cube) {
        CarbonDef.CubeDimension[] dimensions = cube.dimensions;
        CarbonDef.Hierarchy[] hiers =
                ((CarbonDef.Dimension) dimensions[level.getOrdinal()]).hierarchies;
        for (int j = 0; j < hiers.length; j++) {
            CarbonDef.Level[] levels = hiers[j].levels;
            for (int k = 0; k < levels.length; k++) {
                if (level.getName().equals(levels[j].column) && levels[j].visible) {
                    return true;
                }
            }

        }
        return false;
    }

    private static String getMeasures(Cube cube) {
        Measure[] measures = getVisibleMeasures(cube.measures);

        StringBuffer measureStr = new StringBuffer();
        if (null == measures) {
            return measureStr.toString();
        }
        for (int i = 0; i < measures.length; i++) {
            Measure msr = measures[i];
            measureStr.append(msr.aggregator).append("(").append(msr.column).append(")");
            if (i < measures.length - 1) {
                measureStr.append(",");
            }
        }
        return measureStr.toString();
    }

    /**
     * This method will give measures which are enabled
     *
     * @param measures
     * @return
     */
    private static Measure[] getVisibleMeasures(Measure[] measures) {
        if (null == measures) {
            return measures;
        }
        List<Measure> visibleMeasures = new ArrayList<Measure>();
        for (Measure msr : measures) {
            if (msr.visible) {
                visibleMeasures.add(msr);
            }
        }
        return visibleMeasures.toArray(new Measure[visibleMeasures.size()]);
    }

    public static List<String> getDimensionsWithMeasures(List<AggSuggestion> aggSuggest,
            Cube cube) {
        List<String> aggDimensions = new ArrayList<String>(aggSuggest.size());
        for (AggSuggestion aggCombination : aggSuggest) {
            Level[] levels = aggCombination.getAggLevels();
            StringBuffer dims = new StringBuffer();
            for (int i = 0; i < levels.length; i++) {
                dims.append(levels[i].getName());
                if (i < levels.length - 1) {
                    dims.append(",");
                }

            }
            String measureStr = getMeasures(cube);
            //when measure is there than append , at end
            //e.g create aggregate level1,level2,msr1,msr2
            if (measureStr.length() > 0 && dims.length() > 0) {
                dims.append(",");
            }
            dims.append(measureStr);
            aggDimensions.add(dims.toString());
        }
        return aggDimensions;
    }

}

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
package org.carbondata.query.aggregator.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.CalculatedMeasure;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.query.aggregator.CustomMeasureAggregator;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.*;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Class Description : AggUtil class for aggregate classes It will return
 * aggregator instance for measures
 */
public final class AggUtil {

    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(AggUtil.class.getName());
    /**
     * measure ordinal number
     */
    public static int[] measureOrdinal;
    /**
     * all measures
     */
    public static SqlStatement.Type[] allMeasures;
    /**
     * type of Measure
     */
    private static HashMap<Integer, SqlStatement.Type> queryMeasureType = new HashMap<>();

    private AggUtil() {

    }

    /**
     * initialize measureType
     */
    public static void initMeasureType(CarbonQueryExecutorModel queryModel) {
        int ordinal;
        //This part is used to initialize measure types for normal query
        measureOrdinal = new int[queryModel.getMsrs().size()];
        for (int i = 0; i < queryModel.getMsrs().size(); i++) {
            ordinal = queryModel.getMsrs().get(i).getOrdinal();
            queryMeasureType.put(ordinal, queryModel.getMsrs().get(i).getDataType());
            measureOrdinal[i] = ordinal;
        }
        //This psrt is used to initialize all mesure types.
        List<Measure> measures =
                queryModel.getCube().getMeasures(queryModel.getCube().getFactTableName());
        allMeasures = new SqlStatement.Type[measures.size()];
        for (int j = 0; j < measures.size(); j++) {
            allMeasures[j] = measures.get(j).getDataType();
        }
    }

    /**
     * initialize measureType
     */
    public static void initMeasureType(Measure[] measures) {
        int ordinal;
        measureOrdinal = new int[measures.length];
        for (int i = 0; i < measures.length; i++) {
            ordinal = measures[i].getOrdinal();
            queryMeasureType.put(ordinal, measures[i].getDataType());
            measureOrdinal[i] = ordinal;
        }
    }

    /**
     * get selected measureType
     */
    public static SqlStatement.Type getMeasureType(int measureOdinal) {
        if (allMeasures.length != 0) {
            return allMeasures[measureOdinal];
        } else {
            return queryMeasureType.get(measureOdinal);
        }
    }

    /**
     * This method will return aggregate instance based on aggregate name
     *
     * @param aggregatorType aggregator Type
     * @param hasFactCount   whether table has fact count or not
     * @param generator      key generator
     * @return MeasureAggregator
     */
    public static MeasureAggregator getAggregator(String aggregatorType, boolean isNoDictionary,
            boolean hasFactCount, KeyGenerator generator,
            boolean isSurrogateBasedDistinctCountRequired, Object minValue,
            SqlStatement.Type dataType) {
        // this will be used for aggregate table because aggregate tables will
        // have one of the measure as fact count
            return getAggregator(aggregatorType, isNoDictionary, generator,
                    isSurrogateBasedDistinctCountRequired, minValue, dataType);
    }

    /**
     * Factory method, this method determines what agg needs to be given based
     * on MeasureAggregator type
     *
     * @param aggregatorType aggregate name
     * @param generator      key generator
     * @return MeasureAggregator
     */

    /*
        get  Aggregator by dataType and aggregatorType
     */
    private static MeasureAggregator getAggregator(String aggregatorType, boolean isNoDictionary,
            KeyGenerator generator, boolean isSurrogateGeneratedDistinctCount, Object minValue,
            SqlStatement.Type dataType) {
        // get the MeasureAggregator based on aggregate type
        if (CarbonCommonConstants.MIN.equalsIgnoreCase(aggregatorType)) {
            return new MinAggregator();
        } else if (CarbonCommonConstants.COUNT.equalsIgnoreCase(aggregatorType)) {
            return new CountAggregator();
        }
        //
        else if (CarbonCommonConstants.MAX.equalsIgnoreCase(aggregatorType)) {
            return new MaxAggregator();
        }
        //
        else if (CarbonCommonConstants.AVERAGE.equalsIgnoreCase(aggregatorType)) {
            switch (dataType) {
            case LONG:

                return new AvgLongAggregator();
            case DECIMAL:

                return new AvgBigDecimalAggregator();
            default:

                return new AvgDoubleAggregator();
            }
        }
        //
        else if (CarbonCommonConstants.DISTINCT_COUNT.equalsIgnoreCase(aggregatorType)) {
            if (isNoDictionary) {
                return new DistinctStringCountAggregator();
            }
            return new DistinctCountAggregator(minValue);
        } else if (CarbonCommonConstants.SUM.equalsIgnoreCase(aggregatorType)) {
            switch (dataType) {
            case LONG:

                return new SumLongAggregator();
            case DECIMAL:

                return new SumBigDecimalAggregator();
            default:

                return new SumDoubleAggregator();
            }
        } else if (CarbonCommonConstants.SUM_DISTINCT.equalsIgnoreCase(aggregatorType)) {
            switch (dataType) {
            case LONG:

                return new SumDistinctLongAggregator();
            case DECIMAL:

                return new SumDistinctBigDecimalAggregator();
            default:

                return new SumDistinctDoubleAggregator();
            }
        } else if (CarbonCommonConstants.DUMMY.equalsIgnoreCase(aggregatorType)) {
            switch (dataType) {
            case LONG:

                return new DummyLongAggregator();
            case DECIMAL:

                return new DummyBigDecimalAggregator();
            default:

                return new DummyDoubleAggregator();
            }
        } else {
            return null;
        }
    }

    private static MeasureAggregator getCustomAggregator(String aggregatorType,
            String aggregatorClassName, KeyGenerator generator, String cubeUniqueName) {
        //
        try {
            Class customAggregatorClass = Class.forName(aggregatorClassName);
            Constructor declaredConstructor =
                    customAggregatorClass.getDeclaredConstructor(KeyGenerator.class, String.class);
            return (MeasureAggregator) declaredConstructor.newInstance(generator, cubeUniqueName);
        } catch (ClassNotFoundException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "No custom class named " + aggregatorClassName + " was found");
        }
        //
        catch (SecurityException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Security Exception while loading custom class " + aggregatorClassName);
        }
        //
        catch (NoSuchMethodException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "Required constructor for custom class " + aggregatorClassName + " not found");
        } catch (IllegalArgumentException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "IllegalArgumentException while loading custom class " + aggregatorClassName);
        }
        //
        catch (InstantiationException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "InstantiationException while loading custom class " + aggregatorClassName);
        }
        //
        catch (IllegalAccessException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "IllegalAccessException while loading custom class " + aggregatorClassName);
        } catch (InvocationTargetException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
                    "InvocationTargetException while loading custom class " + aggregatorClassName);
        }
        // return default sum aggregator in case something wrong happen and log it
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Custom aggregator could not be loaded, " + "returning the default Sum Aggregator");
        return null;
    }

    /**
     * This method determines what agg needs to be given for each individual
     * measures and based on hasFactCount, suitable avg ... aggs will be choosen
     *
     * @param measures
     * @param hasFactCount
     * @return
     */
    public static MeasureAggregator[] getAggregators(Measure[] measures,
            CalculatedMeasure[] calculatedMeasures, boolean hasFactCount, KeyGenerator generator,
            String slice) {
        int length = measures.length + calculatedMeasures.length;
        MeasureAggregator[] aggregators = new MeasureAggregator[length];
        MeasureAggregator aggregatorInfo = null;
        int cnt = 0;
        for (int i = 0; i < measures.length; i++) {
            if (measures[i].getAggName().equals(CarbonCommonConstants.DISTINCT_COUNT) && measures[i]
                    .isSurrogateGenerated()) {
                cnt++;
            }
        }
        boolean isSurrogateBasedDistinctCountRequired = true;
        if (cnt > 1) {
            isSurrogateBasedDistinctCountRequired = false;
        }
        for (int i = 0; i < measures.length; i++) {
            // based on MeasureAggregator name create the MeasureAggregator
            // object
            String aggName = measures[i].getAggName();
            if (aggName.equals(CarbonCommonConstants.CUSTOM)) {
                aggregatorInfo =
                        getCustomAggregator(aggName, measures[i].getAggClassName(), generator,
                                slice);
            } else {

                aggregatorInfo = getAggregator(aggName, hasFactCount, generator,
                        measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired,
                        measures[i].getMinValue(), measures[i].getDataType());
            }
            aggregators[i] = aggregatorInfo;
        }

        for (int i = 0; i < calculatedMeasures.length; i++) {
            CalculatedMeasureAggregatorImpl aggregatorImpl = new CalculatedMeasureAggregatorImpl();
            aggregators[i + measures.length] = aggregatorImpl;
        }
        return aggregators;
    }

    /**
     * This method determines what agg needs to be given for each aggregateNames
     *
     * @param aggregateNames list of MeasureAggregator name
     * @param hasFactCount   is fact count present
     * @param generator      key generator
     * @return MeasureAggregator array which will hold agrregator for each
     * aggregateNames
     */
    public static MeasureAggregator[] getAggregators(List<String> aggregateNames,
            boolean hasFactCount, KeyGenerator generator, SqlStatement.Type[] dataTypes) {
        int valueSize = aggregateNames.size();
        MeasureAggregator[] aggregators = new MeasureAggregator[valueSize];
        for (int i = 0; i < valueSize; i++) {
            aggregators[i] = getAggregator(aggregateNames.get(i), hasFactCount, generator, false, 0,
                    dataTypes[i]);
        }
        return aggregators;
    }

    /**
     * This method determines what agg needs to be given for each aggregateNames
     *
     * @param aggregateNames list of MeasureAggregator name
     * @param hasFactCount   is fact count present
     * @param generator      key generator
     * @return MeasureAggregator array which will hold agrregator for each
     * aggregateNames
     */
    public static MeasureAggregator[] getAggregators(List<String> aggregateNames,
            List<String> aggregatorClassName, boolean hasFactCount, KeyGenerator generator,
            String cubeUniqueName, Object[] minValue, char[] type) {
        int valueSize = aggregateNames.size();
        MeasureAggregator[] aggregators = new MeasureAggregator[valueSize];
        for (int i = 0; i < valueSize; i++) {
            if (aggregateNames.get(i).equals(CarbonCommonConstants.CUSTOM)) {
                aggregators[i] =
                        getCustomAggregator(aggregateNames.get(i), aggregatorClassName.get(i),
                                generator, cubeUniqueName);
            } else {
                SqlStatement.Type dataType = null;
                switch (type[i]) {
                case 'l':
                    dataType = SqlStatement.Type.LONG;
                    break;
                case 'b':
                    dataType = SqlStatement.Type.DECIMAL;
                    break;
                default:
                    dataType = SqlStatement.Type.DOUBLE;

                }
                aggregators[i] =
                        getAggregator(aggregateNames.get(i), hasFactCount, generator, false,
                                minValue[i], dataType);
            }
        }
        return aggregators;
    }

    public static MeasureAggregator[] getAggregators(String[] aggType, boolean hasFactCount,
            KeyGenerator generator, String cubeUniqueName, Object[] minValue,
            boolean[] noDictionaryTypes, SqlStatement.Type[] dataTypes) {
        MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
        for (int i = 0; i < aggType.length; i++) {
            SqlStatement.Type dataType = dataTypes[i];
            if (null != noDictionaryTypes) {
                aggregators[i] =
                        getAggregator(aggType[i], noDictionaryTypes[i], hasFactCount, generator,
                                false, minValue[i], dataType);
            } else {
                aggregators[i] = getAggregator(aggType[i], false, hasFactCount, generator, false,
                        minValue[i], dataType);
            }
        }
        return aggregators;

    }

    public static MeasureAggregator[] getAggregators(String[] aggType,
            List<CustomCarbonAggregateExpression> aggregateExpressions, boolean hasFactCount,
            KeyGenerator generator, String cubeUniqueName, Object[] minValue,
            boolean[] noDictionaryTypes, SqlStatement.Type[] dataTypes) {
        MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
        int customIndex = 0;
        for (int i = 0; i < aggType.length; i++) {

            if (aggType[i].equalsIgnoreCase(CarbonCommonConstants.CUSTOM)) {
                CustomCarbonAggregateExpression current = aggregateExpressions.get(customIndex++);
                if (current != null && current.getAggregator() != null) {
                    aggregators[i] = (CustomMeasureAggregator) current.getAggregator().getCopy();
                } else {
                    LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                            "Unable to load custom aggregator");
                }
            } else {
                SqlStatement.Type dataType = dataTypes[i];
                if (null != noDictionaryTypes) {
                    aggregators[i] =
                            getAggregator(aggType[i], noDictionaryTypes[i], hasFactCount,
                                    generator, false, minValue[i], dataType);
                } else {
                    aggregators[i] =
                            getAggregator(aggType[i], false, hasFactCount, generator, false,
                                    minValue[i], dataType);
                }
            }
        }
        return aggregators;

    }
}

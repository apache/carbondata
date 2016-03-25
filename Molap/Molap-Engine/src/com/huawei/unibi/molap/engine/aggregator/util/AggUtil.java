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

package com.huawei.unibi.molap.engine.aggregator.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.CustomMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.CustomMolapAggregateExpression;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgBigDecimalAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgDoubleAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgLongAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgOfAvgBigDecimalAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgOfAvgDoubleAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgOfAvgLongAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CalculatedMeasureAggregatorImpl;
import com.huawei.unibi.molap.engine.aggregator.impl.CountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DistinctCountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DistinctStringCountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DummyBigDecimalAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DummyDoubleAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DummyLongAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MaxAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MinAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumBigDecimalAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumDistinctBigDecimalAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumDistinctDoubleAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumDistinctLongAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumDoubleAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumLongAggregator;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.CalculatedMeasure;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.olap.SqlStatement;

/**
 *
 * Class Description : AggUtil class for aggregate classes It will return
 * aggregator instance for measures
 * 
 * Version 1.0
 */
public final class AggUtil
{
    
    private AggUtil()
    {
        
    }
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(AggUtil.class.getName());

    /**
     * type of Measure
     */
    private static HashMap<Integer,SqlStatement.Type> queryMeasureType= new HashMap<>();

    /**
     * measure ordinal number
     */
    public static int[] measureOrdinal;

    /**
     * all measures
     */
    public static SqlStatement.Type[] allMeasures;
    /**
     * initialize measureType
     */
    public static void initMeasureType(MolapQueryExecutorModel queryModel)
    {
        int ordinal;
//This part is used to initialize measure types for normal query
        measureOrdinal = new int[queryModel.getMsrs().size()];
        for(int i = 0; i < queryModel.getMsrs().size();i++)
        {
            ordinal = queryModel.getMsrs().get(i).getOrdinal();
            queryMeasureType.put(ordinal,queryModel.getMsrs().get(i).getDataType());
            measureOrdinal[i] = ordinal;
        }
//This psrt is used to initialize all mesure types.
        List<Measure> measures = queryModel.getCube().getMeasures(queryModel.getCube().getFactTableName());
        allMeasures = new SqlStatement.Type[measures.size()];
        for(int j = 0;j < measures.size();j++)
        {
            allMeasures[j] = measures.get(j).getDataType();
        }
    }

    /**
     * initialize measureType
     */
    public static void initMeasureType(Measure[] measures)
    {
        int ordinal;
        measureOrdinal = new int[measures.length];
        for(int i = 0; i < measures.length;i++)
        {
            ordinal = measures[i].getOrdinal();
            queryMeasureType.put(ordinal, measures[i].getDataType());
            measureOrdinal[i] = ordinal;
        }
    }

    /**
     * get selected measureType
     */
    public static SqlStatement.Type getMeasureType(int measureOdinal)
    {
// return measureType.get(measureOdinal);
        if(allMeasures.length != 0)
        {
            return allMeasures[measureOdinal];
        }
        else
        {
            return queryMeasureType.get(measureOdinal);
        }
    }

    /**
     * This method will return aggregate instance based on aggregate name
     * 
     * @param aggregatorType
     *            aggregator Type
     * @param hasFactCount
     *            whether table has fact count or not
     * @param generator
     *            key generator
     * @param slice
     *            slice
     * @return MeasureAggregator
     * 
     * 
     */
    public static MeasureAggregator getAggregator(String aggregatorType,boolean isHighCardinality, boolean hasFactCount, KeyGenerator generator,
                                                  boolean isSurrogateBasedDistinctCountRequired, Object minValue, SqlStatement.Type dataType)
    {
        // this will be used for aggregate table because aggregate tables will
        // have one of the measure as fact count
        if(hasFactCount && MolapCommonConstants.AVERAGE.equalsIgnoreCase(aggregatorType))
        {
            switch(dataType)
            {
                case LONG:

                    return new AvgOfAvgLongAggregator();
                case DECIMAL:

                    return new AvgOfAvgBigDecimalAggregator();
                default:

                    return new AvgOfAvgDoubleAggregator();
            }
        }
        else
        {
            return getAggregator(aggregatorType, isHighCardinality, generator,isSurrogateBasedDistinctCountRequired, minValue, dataType);
        }
    }

    /**
     * 
     * Factory method, this method determines what agg needs to be given based
     * on MeasureAggregator type
     * 
     * @param aggregatorType
     *            aggregate name
     * @param generator
     *            key generator
     * @param slice
     *            slice
     * @return MeasureAggregator
     * 
     * 
     */

    /*
        get  Aggregator by dataType and aggregatorType
     */
    private static MeasureAggregator getAggregator(String aggregatorType,boolean isHighCardinality, KeyGenerator generator,
                                                   boolean isSurrogateGeneratedDistinctCount, Object minValue, SqlStatement.Type dataType)
    {
        // get the MeasureAggregator based on aggregate type
        if(MolapCommonConstants.MIN.equalsIgnoreCase(aggregatorType))
        {
            return new MinAggregator();
        }
        else if(MolapCommonConstants.COUNT.equalsIgnoreCase(aggregatorType))
        {
            return new CountAggregator();
        }
        //
        else if(MolapCommonConstants.MAX.equalsIgnoreCase(aggregatorType))
        {
            return new MaxAggregator();
        }
        //
        else if(MolapCommonConstants.AVERAGE.equalsIgnoreCase(aggregatorType))
        {
            switch(dataType)
            {
                case LONG:

                    return new AvgLongAggregator();
                case DECIMAL:

                    return new AvgBigDecimalAggregator();
                default:

                    return new AvgDoubleAggregator();
            }
        }
        //
        else if(MolapCommonConstants.DISTINCT_COUNT.equalsIgnoreCase(aggregatorType))
        {
            if(isHighCardinality)
            {
                return new DistinctStringCountAggregator();
            }
            return new DistinctCountAggregator(minValue);
        }
        else if(MolapCommonConstants.SUM.equalsIgnoreCase(aggregatorType))
        {
//            return new SumAggregator();
            switch(dataType)
            {
                case LONG:

                    return new SumLongAggregator();
                case DECIMAL:

                    return new SumBigDecimalAggregator();
                default:

                    return new SumDoubleAggregator();
            }
        }
        else if(MolapCommonConstants.SUM_DISTINCT.equalsIgnoreCase(aggregatorType))
        {
            switch(dataType)
            {
                case LONG:

                    return new SumDistinctLongAggregator();
                case DECIMAL:

                    return new SumDistinctBigDecimalAggregator();
                default:

                    return new SumDistinctDoubleAggregator();
            }
        }
        else if(MolapCommonConstants.DUMMY.equalsIgnoreCase(aggregatorType))
        {
            switch(dataType)
            {
                case LONG:

                    return new DummyLongAggregator();
                case DECIMAL:

                    return new DummyBigDecimalAggregator();
                default:

                    return new DummyDoubleAggregator();
            }
        }
        else
        {
            return null;
        }
    }
    
    private static MeasureAggregator getCustomAggregator(String aggregatorType, String aggregatorClassName, KeyGenerator generator, String cubeUniqueName)
    {
        //
        try
        {
            Class customAggregatorClass = Class.forName(aggregatorClassName);
            Constructor declaredConstructor = customAggregatorClass.getDeclaredConstructor(KeyGenerator.class,String.class);
            return (MeasureAggregator)declaredConstructor.newInstance(generator,cubeUniqueName);
        }
        catch(ClassNotFoundException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "No custom class named " 
                    + aggregatorClassName + " was found");
        }
        //
        catch(SecurityException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "Security Exception while loading custom class " 
                    + aggregatorClassName);
        }
        //
        catch(NoSuchMethodException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "Required constructor for custom class " 
                    + aggregatorClassName + " not found");
        }
        catch(IllegalArgumentException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "IllegalArgumentException while loading custom class " 
                    + aggregatorClassName);
        }
        //
        catch(InstantiationException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "InstantiationException while loading custom class " 
                    + aggregatorClassName);
        }
        //
        catch(IllegalAccessException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "IllegalAccessException while loading custom class " 
                    + aggregatorClassName);
        }
        catch(InvocationTargetException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, 
                    "InvocationTargetException while loading custom class " 
                    + aggregatorClassName);
        }
        // return default sum aggregator in case something wrong happen and log it
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,  
                "Custom aggregator could not be loaded, " +
                "returning the default Sum Aggregator");
        return null;
    }

    /**
     * This method determines what agg needs to be given for each individual
     * measures and based on hasFactCount, suitable avg ... aggs will be choosen
     * 
     * @param measures
     * @param hasFactCount
     * @return
     * 
     */
    public static MeasureAggregator[] getAggregatorsWithCubeName(Measure[] measures, boolean hasFactCount, KeyGenerator generator,
            String cubeUniqueName)
    {
        MeasureAggregator[] aggregators = new MeasureAggregator[measures.length];
        
        int count=0;
        for(int i = 0;i < measures.length;i++)
        {
            if(measures[i].getAggName().equals(MolapCommonConstants.DISTINCT_COUNT)
                    && measures[i].isSurrogateGenerated())
            {
                count++;
            }
        }
        boolean isSurrogateBasedDistinctCountRequired = true;
        if(count > 1)
        {
            isSurrogateBasedDistinctCountRequired = false;
        }
        
        MeasureAggregator aggregator = null;
        for(int i = 0;i < measures.length;i++)
        {
            // based on MeasureAggregator name create the MeasureAggregator
            // object
            String aggName = measures[i].getAggName();
            if(aggName.equals(MolapCommonConstants.CUSTOM))
            {
                aggregator = getCustomAggregator(aggName, measures[i].getAggClassName(), generator, cubeUniqueName);
            }
            else
            {
                
                aggregator = getAggregator(aggName, hasFactCount, generator, measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired,
                        measures[i].getMinValue(), measures[i].getDataType());
            }
//            if (aggregator == null) {
//                 throw MondrianResource.instance().UnknownAggregator.ex(
//                         aggName," Aggregator not found");
//            }
            aggregators[i] = aggregator;
        }
        return aggregators;
    }
    
    /**
     * This method determines what agg needs to be given for each individual
     * measures and based on hasFactCount, suitable avg ... aggs will be choosen
     * 
     * @param measures
     * @param hasFactCount
     * @return
     * 
     */
    public static MeasureAggregator[] getAggregators(Measure[] measures,MolapCalcFunction[] calculatedMeasures, boolean hasFactCount, KeyGenerator generator,
            String slice)
    {
        int length = measures.length+calculatedMeasures.length;
        MeasureAggregator[] aggregators = new MeasureAggregator[length];
        MeasureAggregator aggregator = null;
        int count=0;
        for(int i = 0;i < measures.length;i++)
        {
            if(measures[i].getAggName().equals(MolapCommonConstants.DISTINCT_COUNT)
                    && measures[i].isSurrogateGenerated())
            {
                count++;
            }
        }
        boolean isSurrogateBasedDistinctCountRequired = true;
        if(count > 1)
        {
            isSurrogateBasedDistinctCountRequired = false;
        }
        for(int i = 0;i < measures.length;i++)
        {
            // based on MeasureAggregator name create the MeasureAggregator
            // object
            String aggName = measures[i].getAggName();
            if(aggName.equals(MolapCommonConstants.CUSTOM))
            {
                aggregator = getCustomAggregator(aggName, measures[i].getAggClassName(), generator, slice);
            }
            else
            {
                
                aggregator = getAggregator(aggName, hasFactCount, generator,measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired,
                        measures[i].getMinValue(), measures[i].getDataType());
            }
//            if (aggregator == null) {
//                 throw MondrianResource.instance().UnknownAggregator.ex(
//                         aggName," Aggregator not found");
//            }
            aggregators[i] = aggregator;
        }
        
        for(int i = 0;i < calculatedMeasures.length;i++)
        {
            CalculatedMeasureAggregatorImpl aggregatorImpl = new CalculatedMeasureAggregatorImpl(calculatedMeasures[i]);
            aggregators[i+measures.length] = aggregatorImpl;
        }
        return aggregators;
    }
    
    /**
     * This method determines what agg needs to be given for each individual
     * measures and based on hasFactCount, suitable avg ... aggs will be choosen
     * 
     * @param measures
     * @param hasFactCount
     * @return
     * 
     */
    public static MeasureAggregator[] getAggregators(Measure[] measures,CalculatedMeasure[] calculatedMeasures, boolean hasFactCount, KeyGenerator generator,
            String slice)
    {
        int length = measures.length+calculatedMeasures.length;
        MeasureAggregator[] aggregators = new MeasureAggregator[length];
        MeasureAggregator aggregatorInfo = null;
        int cnt=0;
        for(int i = 0;i < measures.length;i++)
        {
            if(measures[i].getAggName().equals(MolapCommonConstants.DISTINCT_COUNT)
                    && measures[i].isSurrogateGenerated())
            {
                cnt++;
            }
        }
        boolean isSurrogateBasedDistinctCountRequired = true;
        if(cnt > 1)
        {
            isSurrogateBasedDistinctCountRequired = false; 
        }
        for(int i = 0;i < measures.length;i++) 
        {
            // based on MeasureAggregator name create the MeasureAggregator
            // object
            String aggName = measures[i].getAggName();
            if(aggName.equals(MolapCommonConstants.CUSTOM))
            {
                aggregatorInfo = getCustomAggregator(aggName, measures[i].getAggClassName(), generator, slice);
            }
            else
            {
                
                aggregatorInfo = getAggregator(aggName, hasFactCount, generator,measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired,
                        measures[i].getMinValue(), measures[i].getDataType());
            }
//            if (aggregator == null) {
//                 throw MondrianResource.instance().UnknownAggregator.ex(
//                         aggName," Aggregator not found");
//            }
            aggregators[i] = aggregatorInfo;
        }
        
        for(int i = 0;i < calculatedMeasures.length;i++)
        {
            CalculatedMeasureAggregatorImpl aggregatorImpl = new CalculatedMeasureAggregatorImpl();
            aggregators[i+measures.length] = aggregatorImpl;
        }
        return aggregators;
    }

    /**
     * This method determines what agg needs to be given for each aggregateNames
     * 
     * @param aggregateNames
     *            list of MeasureAggregator name
     * @param hasFactCount
     *            is fact count present
     * @param generator
     *            key generator
     * @param slice
     *            slice
     * @return MeasureAggregator array which will hold agrregator for each
     *         aggregateNames
     * 
     */
    public static MeasureAggregator[] getAggregators(List<String> aggregateNames, boolean hasFactCount,
                                                     KeyGenerator generator, SqlStatement.Type[] dataTypes)
    {
        int valueSize = aggregateNames.size();
        MeasureAggregator[] aggregators = new MeasureAggregator[valueSize];
        for(int i = 0;i < valueSize;i++)
        {
            aggregators[i] = getAggregator(aggregateNames.get(i), hasFactCount, generator,false, 0, dataTypes[i]);
        }
        return aggregators;
    }
    
    /**
     * This method determines what agg needs to be given for each aggregateNames
     * 
     * @param aggregateNames
     *            list of MeasureAggregator name
     * @param hasFactCount
     *            is fact count present
     * @param generator
     *            key generator
     * @param slice
     *            slice
     * @return MeasureAggregator array which will hold agrregator for each
     *         aggregateNames
     * 
     */
    public static MeasureAggregator[] getAggregators(List<String> aggregateNames, List<String> aggregatorClassName,boolean hasFactCount,
                                                     KeyGenerator generator,String cubeUniqueName, Object[] minValue, char[] type)
    {
        int valueSize = aggregateNames.size();
        MeasureAggregator[] aggregators = new MeasureAggregator[valueSize];
        for(int i = 0;i < valueSize;i++)
        {
            if(aggregateNames.get(i).equals(MolapCommonConstants.CUSTOM))
            {
                aggregators[i]=getCustomAggregator(aggregateNames.get(i), aggregatorClassName.get(i), generator, cubeUniqueName);
            }
            else
            {
                SqlStatement.Type dataType = null;
                switch (type[i])
                {
                    case 'l':
                        dataType = SqlStatement.Type.LONG;
                        break;
                    case 'b':
                        dataType = SqlStatement.Type.DECIMAL;
                        break;
                    default:
                        dataType = SqlStatement.Type.DOUBLE;

                }
                aggregators[i] = getAggregator(aggregateNames.get(i), hasFactCount, generator,false,minValue[i], dataType);
            }
        }
        return aggregators;
    }
/**
     * 
 * @param aggType
 * @param hasFactCount
 * @param generator
 * @param cubeUniqueName
 * @param minValue
 * @param highCardinalityTypes
 * @param dataTypes
 * @return
     */
    public static MeasureAggregator[] getAggregators(String[] aggType, boolean hasFactCount,
                                                     KeyGenerator generator, String cubeUniqueName, Object[] minValue,boolean[] highCardinalityTypes, SqlStatement.Type[] dataTypes)
    {
        MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
        for(int i = 0;i < aggType.length;i++)
        {
            SqlStatement.Type dataType = dataTypes[i];
            if(null!=highCardinalityTypes)
            {
            aggregators[i]  = getAggregator(aggType[i],highCardinalityTypes[i], hasFactCount, generator, false, minValue[i], dataType);
            }
            else
            {
                aggregators[i]  = getAggregator(aggType[i],false, hasFactCount, generator, false, minValue[i], dataType);
            }
        }
        return aggregators;
        
    }
/**
 * 
 * @param aggType
 * @param aggregateExpressions
 * @param hasFactCount
 * @param generator
 * @param cubeUniqueName
 * @param minValue
 * @param highCardinalityTypes
 * @param dataTypes
 * @return
 */
    public static MeasureAggregator[] getAggregators(String[] aggType,
            List<CustomMolapAggregateExpression> aggregateExpressions, boolean hasFactCount, KeyGenerator generator,
            String cubeUniqueName, Object[] minValue,boolean[] highCardinalityTypes, SqlStatement.Type[] dataTypes)
    {
        MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
        int customIndex=0;
        for(int i = 0;i < aggType.length;i++)
        {

            if(aggType[i].equalsIgnoreCase(MolapCommonConstants.CUSTOM))
            {
                CustomMolapAggregateExpression current = aggregateExpressions.get(customIndex++);
                if(current != null && current.getAggregator()!=null)
                {
                    aggregators[i] = (CustomMeasureAggregator)current.getAggregator().getCopy();
                }
                else
                {
                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Unable to load custom aggregator");
                }
            }
            else
            {
				SqlStatement.Type dataType = dataTypes[i];
                if(null != highCardinalityTypes)
                {
                    aggregators[i] = getAggregator(aggType[i],highCardinalityTypes[i], hasFactCount, generator, false, minValue[i], dataType);
                }
                else
                {
                    aggregators[i] = getAggregator(aggType[i],false, hasFactCount, generator, false, minValue[i], dataType);
                }
            }
        }
        return aggregators;
    
    }
}

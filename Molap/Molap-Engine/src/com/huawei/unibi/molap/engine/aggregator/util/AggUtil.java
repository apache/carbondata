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
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.CustomMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.CustomMolapAggregateExpression;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AvgOfAvgAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CalculatedMeasureAggregatorImpl;
import com.huawei.unibi.molap.engine.aggregator.impl.CountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DistinctCountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DistinctCountAggregatorObjectSet;
import com.huawei.unibi.molap.engine.aggregator.impl.DistinctStringCountAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.DummyAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MaxAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.MinAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SumDistinctAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.SurrogateBasedDistinctCountAggregator;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.CalculatedMeasure;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

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
     * 
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
           boolean isSurrogateBasedDistinctCountRequired, double minValue) 
    {
        // this will be used for aggregate table because aggregate tables will
        // have one of the measure as fact count
        if(hasFactCount && MolapCommonConstants.AVERAGE.equalsIgnoreCase(aggregatorType))
        {
            return new AvgOfAvgAggregator();
        }
        else
        {
            return getAggregator(aggregatorType,isHighCardinality, generator,isSurrogateBasedDistinctCountRequired, minValue);
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
    private static MeasureAggregator getAggregator(String aggregatorType,boolean isHighCardinality, KeyGenerator generator,boolean isSurrogateGeneratedDistinctCount, double minValue)
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
            return new AvgAggregator();
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
            return new SumAggregator();
        }
        else if(MolapCommonConstants.SUM_DISTINCT.equalsIgnoreCase(aggregatorType))
        {
            return new SumDistinctAggregator();
        }
        else if(MolapCommonConstants.DUMMY.equalsIgnoreCase(aggregatorType))
        {
            return new DummyAggregator();
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
                
                aggregator = getAggregator(aggName, hasFactCount, generator, measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired, measures[i].getMinValue());
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
                
                aggregator = getAggregator(aggName, hasFactCount, generator,measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired, measures[i].getMinValue());
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
                
                aggregatorInfo = getAggregator(aggName, hasFactCount, generator,measures[i].isSurrogateGenerated() && isSurrogateBasedDistinctCountRequired, measures[i].getMinValue());
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
            KeyGenerator generator)
    {
        int valueSize = aggregateNames.size();
        MeasureAggregator[] aggregators = new MeasureAggregator[valueSize];
        for(int i = 0;i < valueSize;i++)
        {
            aggregators[i] = getAggregator(aggregateNames.get(i), hasFactCount, generator,false, 0);
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
            KeyGenerator generator,String cubeUniqueName, double[] minValue)
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
                aggregators[i] = getAggregator(aggregateNames.get(i), hasFactCount, generator,false,minValue[i]);
            }
        }
        return aggregators;
    }
    
    /**
     * 
     * This method will aggregate values to aggreator passed as a parameter and
     * return the aggregated value
     * 
     * @param measureAggregator
     * @param value1
     * @param value2
     * @param factCount
     * @param factCount2
     * @return return aggreagted value
     * 
     */
    public static double aggregate(MeasureAggregator measureAggregator, double value1, double value2,
            double factCount, double factCount2)  
    {
        // aggregate first value
        measureAggregator.agg(value1, factCount); 
        // aggregate second value
        measureAggregator.agg(value2, factCount2);
        // return the aggregated values
        return measureAggregator.getValue();
    }

    public static MeasureAggregator[] getAggregators(String[] aggType, boolean hasFactCount,
            KeyGenerator generator, String cubeUniqueName, double[] minValue,boolean[] highCardinalityTypes)
    {
        MeasureAggregator[] aggregators = new MeasureAggregator[aggType.length];
        for(int i = 0;i < aggType.length;i++)
        {
            if(null!=highCardinalityTypes)
            {
            aggregators[i]  = getAggregator(aggType[i],highCardinalityTypes[i], hasFactCount, generator, false, minValue[i]);
            }
            else
            {
                aggregators[i]  = getAggregator(aggType[i],false, hasFactCount, generator, false, minValue[i]);
            }
        }
        return aggregators;
        
    }
    
    public static MeasureAggregator[] getAggregators(String[] aggType,
            List<CustomMolapAggregateExpression> aggregateExpressions, boolean hasFactCount, KeyGenerator generator,
            String cubeUniqueName, double[] minValue,boolean[] highCardinalityTypes)
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
                if(null != highCardinalityTypes)
                {
                    aggregators[i] = getAggregator(aggType[i],highCardinalityTypes[i], hasFactCount, generator, false, minValue[i]);
                }
                else
                {
                    aggregators[i] = getAggregator(aggType[i],false, hasFactCount, generator, false, minValue[i]);
                }
            }
        }
        return aggregators;
    
    }
}

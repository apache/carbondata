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

package com.huawei.unibi.molap.engine.querystats;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * This class analyze history query and checks if it meets the criteria. If it meets than it will add to normalized query list 
 * @author A00902717
 *
 */
public class QueryNormalizer
{
    
   
    private List<QueryDetail> normalizedQueryDetails = new ArrayList<QueryDetail>(1000);
    
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(QueryNormalizer.class.getName());
    
    /**
     * If any logged query is expired than rewrite the log by removing expired query
     */
    private boolean reWriteQueryStats;
      
    private int benefitRatio=Preference.BENEFIT_RATIO;
    
    private int performanceGoal=Preference.PERFORMANCE_GOAL;
    
    /**
     * How old query should be considered
     */
    private int queryExpiryDays=Preference.QUERY_EXPIRY_DAYS;
    
    public QueryNormalizer()
    {
        String confPerformanceGoal=MolapProperties.getInstance().getProperty(Preference.PERFORMANCE_GOAL_KEY);
        if(null!=confPerformanceGoal)
        {
            performanceGoal=Integer.parseInt(confPerformanceGoal);
        }
        String confQueryExpiryDay=MolapProperties.getInstance().getProperty(Preference.QUERY_STATS_EXPIRY_DAYS_KEY);
        if(null!=confQueryExpiryDay)
        {
            queryExpiryDays=Integer.parseInt(confQueryExpiryDay);
        }
        String confBenefitRatio=MolapProperties.getInstance().getProperty(Preference.BENEFIT_RATIO_KEY);
        
        if(null!=confBenefitRatio)
        {
            benefitRatio=Integer.parseInt(confBenefitRatio);            
        }
        
    }
    public boolean addQueryDetail(QueryDetail queryDetail)
    {
        if(queryDetail.getRecordSize()==0)
        {
            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Filtering query as its result size is 0");
            return false;
        }
        int actualBenefitRatio = (int)(queryDetail.getNoOfRowsScanned() / queryDetail.getRecordSize());
        if(actualBenefitRatio<benefitRatio)
        {
            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Filtering query as it doesn't meet benefit ration criteria");
            return false;
        }
        queryDetail.setBenefitRatio(actualBenefitRatio);
        // if current query already exist than increase its frequency
        // and set lowest execution time of both
        int indx = normalizedQueryDetails.indexOf(queryDetail);
        if(indx != -1)
        {
            QueryDetail existingQuery = normalizedQueryDetails.get(indx);
            existingQuery.incrementFrequency();
            if(existingQuery.getTotalExecutionTime() > queryDetail.getTotalExecutionTime())
            {
                existingQuery.setTotalExecutionTime(queryDetail.getTotalExecutionTime());
                
            }
            //if latest query record size is more than previous query record size, than consider record size of current query
            if(queryDetail.getRecordSize()>existingQuery.getRecordSize())
            {
                existingQuery.setRecordSize(queryDetail.getRecordSize());
            }
        }
        else
        {
         
            if(isExpired(queryDetail.getQueryStartTime()))
            {
                reWriteQueryStats = true;
                return false;
            }
            else 
            {
                normalizedQueryDetails.add(queryDetail);
            }
            
        }
        return true;
    }
    
    public boolean isReWriteRequired()
    {
        return reWriteQueryStats;
    }
    
    /**
     * if query time is older than 30 days and it's expired and should not be
     * considered
     * 
     * @param queryTime
     * @return
     */
    private boolean isExpired(long queryTime)
    {
        Calendar expDay = Calendar.getInstance();
        expDay.add(Calendar.DAY_OF_YEAR, -queryExpiryDays);

        if(queryTime < expDay.getTimeInMillis())
        {
            return true;
        }
        return false;
    }
    
    /**
     * Calculate some parameter based on which aggregate combination will be decided
     * @return
     */
    public List<QueryDetail> getNormalizedQueries()
    {
        Iterator<QueryDetail> itr=normalizedQueryDetails.iterator();
        while(itr.hasNext())
        {
            QueryDetail queryDetail=itr.next();
            long performanceGap=TimeUnit.MILLISECONDS.toSeconds(queryDetail.getTotalExecutionTime())-performanceGoal;
            if(performanceGap<0)
            {
                LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Filtering query as it doesn't meet performance goal criteria");
                itr.remove();
                continue;
            }
            long freqWeightedPerfGap=performanceGap*queryDetail.getFrequency();
            long weightage= queryDetail.getBenefitRatio()*freqWeightedPerfGap;
            queryDetail.setWeightage(weightage);
        }
        
        return normalizedQueryDetails;
    }
   
   

}

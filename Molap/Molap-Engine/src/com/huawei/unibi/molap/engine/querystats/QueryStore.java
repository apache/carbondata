package com.huawei.unibi.molap.engine.querystats;

import java.io.Serializable;
import java.util.List;

public interface QueryStore extends Serializable
{
     void logQuery(QueryDetail queryDetail);
    
     QueryDetail[] readQueryDetail(String queryStatsPath);
     
     void writeQueryToFile(List<QueryDetail> queryDetails,String queryStatsPath);
    
}

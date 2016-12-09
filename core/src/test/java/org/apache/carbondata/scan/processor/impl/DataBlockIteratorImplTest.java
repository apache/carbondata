package org.apache.carbondata.scan.processor.impl;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.scan.collector.ScannedResultCollector;
import org.apache.carbondata.scan.collector.impl.DictionaryBasedResultCollector;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by deepak on 7/11/16.
 */
public class DataBlockIteratorImplTest {
private static DataBlockIteratorImpl dataBlockIteratorImpl;
    private BlockExecutionInfo blockExecutionInfo;
    private FileHolder fileHolder;
private QueryStatisticsModel queryStatisticsModel;
    public void setUp(){
        int batchSize = 2;
        dataBlockIteratorImpl = new DataBlockIteratorImpl(blockExecutionInfo,fileHolder,batchSize,queryStatisticsModel);
    }
/*
    @Test
    public void testNext(){
new MockUp<DictionaryBasedResultCollector>(){
    @Mock
     public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {
 List<Object[]> list = new ArrayList<>();
        list.add(0,new Integer[]{1,2});
        list.add(1,new Integer[]{2,4});
   return  list;
}};
        dataBlockIteratorImpl.next();

    }*/
}

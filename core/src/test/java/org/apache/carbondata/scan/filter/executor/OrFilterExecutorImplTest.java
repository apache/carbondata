package org.apache.carbondata.scan.filter.executor;

import org.apache.carbondata.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.scan.filter.executer.OrFilterExecuterImpl;
import org.junit.Test;

/**
 * Created by deepak on 7/11/16.
 */
public class OrFilterExecutorImplTest {
    private static OrFilterExecuterImpl orFilterExecuterImpl;
     private static FilterExecuter leftExecutor;
    private static FilterExecuter rightExecuter;

    public static void setUp(){
        orFilterExecuterImpl = new OrFilterExecuterImpl(leftExecutor,rightExecuter);
    }

   /* @Test
    public void testIsScanRequired(){
        byte[][] blockMaxValue = {{1,1},{1,2}};
        byte[][] blockMinValue = {{0,0},{1,1}};
        orFilterExecuterImpl.isScanRequired(blockMaxValue,blockMinValue);
    }*/
}

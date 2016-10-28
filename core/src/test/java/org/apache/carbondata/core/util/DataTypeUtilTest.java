package org.apache.carbondata.core.util;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static org.apache.carbondata.core.util.DataTypeUtil.*;
import static junit.framework.TestCase.*;
/**
 * Created by knoldus on 28/10/16.
 */
public class DataTypeUtilTest {


    @Test
    public void testGetColumnDataTypeDisplayName(){
        String expected = DataType.INT.getName();
        String result = getColumnDataTypeDisplayName("INT");
        assertEquals(expected, result);

    }

    @Test
    public void testByteToBigDecimal(){
        byte [] byteArr = {0,0};
        byte[] unscale = new byte[byteArr.length - 1];
        BigInteger bigInteger = new BigInteger(unscale);
        BigDecimal expected = new BigDecimal(bigInteger,0);
        BigDecimal result =  byteToBigDecimal(byteArr);
        assertEquals(expected,result);

    }

    @Test
    public void testGetAggType(){
        assertTrue(getAggType(DataType.DECIMAL)=='b');
        assertTrue(getAggType(DataType.INT)=='l');
        assertTrue(getAggType(DataType.LONG)=='l');
    }

    
}



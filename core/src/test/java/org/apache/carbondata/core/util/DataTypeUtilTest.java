package org.apache.carbondata.core.util;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
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

    @Test
    public void testBigDecimalToByte() {
        byte [] result = bigDecimalToByte(BigDecimal.ONE);
        assertTrue(result == result);
    }

    @Test
    public void testGetDataType(){
        assertEquals(DataType.TIMESTAMP,getDataType("TIMESTAMP"));
        assertEquals(DataType.STRING,getDataType("STRING"));
        assertEquals(DataType.INT,getDataType("INT"));
        assertEquals(DataType.SHORT,getDataType("SHORT"));
        assertEquals(DataType.LONG,getDataType("LONG"));
        assertEquals(DataType.DOUBLE,getDataType("DOUBLE"));
        assertEquals(DataType.DECIMAL,getDataType("DECIMAL"));
        assertEquals(DataType.ARRAY,getDataType("ARRAY"));
        assertEquals(DataType.STRUCT,getDataType("STRUCT"));
        assertEquals(DataType.STRING,getDataType("MAP"));
    }

    @Test
    public void testGetDataBasedOnDataType() throws Exception{
        assertEquals(getDataBasedOnDataType("1",DataType.INT),1);
        //assertEquals(getDataBasedOnDataType("0",DataType.SHORT),0);
        assertEquals(getDataBasedOnDataType("0",DataType.DOUBLE),0.0d);
        assertEquals(getDataBasedOnDataType("0",DataType.LONG),0L);
        java.math.BigDecimal javaDecVal = new java.math.BigDecimal(1);
        scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
        org.apache.spark.sql.types.Decimal expected = new org.apache.spark.sql.types.Decimal().set(scalaDecVal);
        assertEquals(getDataBasedOnDataType("1",DataType.DECIMAL),expected);
    }

    @Test
    public void testGetMeasureDataBasedOnDataType() throws Exception {
        Object object = new Object();
        assertEquals(getMeasureDataBasedOnDataType(object,DataType.LONG),object);
        assertEquals(getMeasureDataBasedOnDataType(object,DataType.DOUBLE),object);
        java.math.BigDecimal javaDecVal = new java.math.BigDecimal(1);
        scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
        org.apache.spark.sql.types.Decimal expected = new org.apache.spark.sql.types.Decimal().set(scalaDecVal);
        assertEquals(getMeasureDataBasedOnDataType(1,DataType.DECIMAL),expected);
    }

}



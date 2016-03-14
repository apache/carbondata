package com.huawei.datasight.testsuite.aggquery;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite from where all the TestCases for aggregate query support will be triggered
 * @author N00902756
 *
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	AllDataTypesTestCase.class, 
	IntegerDataTypeTestCase.class, 
	StringDataTypeTestCase.class, 
	TimestampDataTypeTestCase.class, 
	NumericDataTypeTestCase.class
})
public class AggQueryTestSuite {

}

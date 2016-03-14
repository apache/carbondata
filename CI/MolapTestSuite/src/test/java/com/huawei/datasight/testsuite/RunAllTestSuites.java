package com.huawei.datasight.testsuite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.huawei.datasight.testsuite.aggquery.AggQueryTestSuite;
import com.huawei.datasight.testsuite.detailquery.DetailQueryTestSuite;
import com.huawei.datasight.testsuite.filterexpr.FilterExprTestSuite;
import com.huawei.datasight.testsuite.joinquery.JoinQueryTestSuite;
import com.huawei.datasight.testsuite.sortexpr.SortExprTestSuite;

/**
 * Test Suite which calls each individual TestSuite
 * @author N00902756
 *
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	AggQueryTestSuite.class, 
	DetailQueryTestSuite.class, 
	FilterExprTestSuite.class, 
	JoinQueryTestSuite.class, 
	SortExprTestSuite.class
})
public class RunAllTestSuites {

}

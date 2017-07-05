/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.cluster.sdv.suite

import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.{BeforeAndAfterAll, Suites}

import org.apache.carbondata.cluster.sdv.generated._
import org.apache.carbondata.spark.testsuite.sdv.generated._

/**
 * Suite class for all tests.
 */
class SDVSuites extends Suites with BeforeAndAfterAll {

  val suites = new ARRAYCOMTestCase ::
               new NULLTABLETestCase ::
               new ARRAYOFSTRUCTCOMTestCase ::
               new CARBONAUTOMATIONTestCase ::
               new CMBTestCase ::
               new COMPTABLEONEJOINTestCase ::
               new COMPVMALLDICTIONARYCOLUMNGRPTestCase ::
               new COMPVMALLDICTIONARYEXCLUDETestCase ::
               new COMPVMALLDICTIONARYINCLUDETestCase ::
               new COMPVMALLDICTIONARYSHARECOMPTestCase ::
//               new MYVMALLTestCase ::
//               new ORIGINTABLE1TestCase ::
//               new ORIGINTABLE2TestCase ::
//               new OSCONCARBONVIPTestCase ::
//               new SEQUENTIALTestCase ::
//               new SMART500DETestCase ::
//               new SMART500DINCTestCase ::
//               new SORTTABLE1TestCase ::
//               new SORTTABLE2TestCase ::
//               new SORTTABLE3TestCase ::
//               new SORTTABLE4HEAPINMEMORYTestCase ::
//               new SORTTABLE4HEAPSAFETestCase ::
//               new SORTTABLE4HEAPUNSAFETestCase ::
//               new SORTTABLE4OFFHEAPINMEMORYTestCase ::
//               new SORTTABLE4OFFHEAPSAFETestCase ::
//               new SORTTABLE4OFFHEAPUNSAFETestCase ::
//               new SORTTABLE5TestCase ::
//               new SORTTABLE6TestCase ::
//               new SOURCEUNIQDATATestCase ::
//               new STRUCTCOMTestCase ::
//               new STRUCTOFARRAYCOMTestCase ::
//               new STUDENT1INTestCase ::
//               new TARGETUNIQDATATestCase ::
//               new TESTBOUNDARY1TestCase ::
//               new TRAFFIC2G3G4GVMALLTestCase ::
//               new UNIQDATA1MBTestCase ::
//               new UNIQDATAASPTestCase ::
//               new UNIQDATACHAR1TestCase ::
//               new UNIQDATACHARTestCase ::
//               new UNIQDATADATEDICTIONARYTestCase ::
//               new UNIQDATADATETestCase ::
//               new UNIQDATAEXCLUDEDICTIONARYTestCase ::
//               new UNIQDATAINCLUDEDICTIONARY1SPTestCase ::
//               new UNIQDATAINCLUDEDICTIONARY2SPTestCase ::
//               new UNIQDATAINCLUDEDICTIONARYSPTestCase ::
//               new UNIQDATAINCLUDEDICTIONARYTestCase ::
//               new UNIQDATATestCase ::
//               new UNIQEXCLUDE1SPTestCase ::
//               new UNIQEXCLUDESPTestCase ::
//               new UNIQINCLUDEDICTIONARYCOMPMAJORSPTestCase ::
//               new UNIQINCLUDEDICTIONARYCOMPMINORSPTestCase ::
//               new UNIQINCLUDEDICTIONARYSPTestCase ::
//               new UNIQSHAREDDICTIONARYSPTestCase ::
//               new VMALLDICTIONARYCOLUMNGRPTestCase ::
//               new VMALLDICTIONARYEXCLUDETestCase ::
//               new VMALLDICTIONARYINCLUDETestCase ::
               Nil

  override val nestedSuites = suites.toIndexedSeq

  override protected def afterAll() = {
    println("---------------- Stopping spark -----------------")
    TestQueryExecutor.INSTANCE.stop()
    println("---------------- Stopped spark -----------------")
    System.exit(0)
  }
}

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

package org.apache.carbondata.flink;

import org.apache.carbondata.flink.utils.FlinkTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.spark.sql.CarbonContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CarbonFlinkInputFormatPerformanceTest {

    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatPerformanceTest.class.getName());
    Date date = new Date();
    private static FlinkTestUtils flinkTestUtils = new FlinkTestUtils();


    static String getRootPath() throws IOException {
        return new File(CarbonFlinkInputFormatPerformanceTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    private static String getCreateTableCommand(String tableName) {
        return "CREATE TABLE "+ tableName +" (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES (\"TABLE_BLOCKSIZE\"= \"256 MB\")";
    }

    private static String getLoadTableCommand(String filePath, String tableName) {
        return "LOAD DATA INPATH '"+ filePath + "' into table "+ tableName +" OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')";
    }

    @BeforeClass
    public static void defineStore() throws IOException {
        CarbonContext carbonContext = flinkTestUtils.createCarbonContext();

        String inputDataForHundred = getRootPath() + "/integration/flink/src/test/resources/100_UniqData.csv";
        String createTableCommandForHundred = getCreateTableCommand("100_uniqdata");
        String loadTableCommandForHundred = getLoadTableCommand(inputDataForHundred, "100_uniqdata");
        flinkTestUtils.createStore(carbonContext, createTableCommandForHundred, loadTableCommandForHundred);

        String inputDataForThousand = getRootPath() + "/integration/flink/src/test/resources/1000_UniqData.csv";
        String createTableCommandForThousand = getCreateTableCommand("1000_uniqdata");
        String loadTableCommandForThousand = getLoadTableCommand(inputDataForThousand, "1000_uniqdata");
        flinkTestUtils.createStore(carbonContext, createTableCommandForThousand, loadTableCommandForThousand);

        String inputDataForTenThousand = getRootPath() + "/integration/flink/src/test/resources/10000_UniqData.csv";
        String createTableCommandForTenThousand = getCreateTableCommand("10000_uniqdata");
        String loadTableCommandForTenThousand = getLoadTableCommand(inputDataForTenThousand, "10000_uniqdata");
        flinkTestUtils.createStore(carbonContext, createTableCommandForTenThousand, loadTableCommandForTenThousand);

        String inputDataForLakh = getRootPath() + "/integration/flink/src/test/resources/1lac_UniqData.csv";
        String createTableCommandForLakh = getCreateTableCommand("1lac_uniqdata");
        String loadTableCommandForLakh = getLoadTableCommand(inputDataForLakh, "1lac_uniqdata");
        flinkTestUtils.createStore(carbonContext, createTableCommandForLakh, loadTableCommandForLakh);

        String inputDataForFourLakh = getRootPath() + "/integration/flink/src/test/resources/4lac_UniqData.csv";
        String createTableCommandForFourLakh = getCreateTableCommand("4lac_uniqdata");
        String loadTableCommandForFourLakh = getLoadTableCommand(inputDataForFourLakh, "4lac_uniqdata");
        flinkTestUtils.createStore(carbonContext, createTableCommandForFourLakh, loadTableCommandForFourLakh);

        flinkTestUtils.closeContext(carbonContext);
    }

    String getPerformanceReportFilePath() throws IOException {
        return getRootPath() + "/integration/flink/target/performance-report-input-format.txt";
    }

    private Boolean writeToFile(String content) {
        try {
            FileWriter writer = new FileWriter(getPerformanceReportFilePath(), true);
            writer.write(content);
            writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Test
    public void generatePerformanceReport() throws Exception {
        LOGGER.info("\n\n Writing Performance Report to : " + getPerformanceReportFilePath() + "\n\n");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");
        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");

        LOGGER.info(">>>>>>>>>Hundred records:::::::::::::::::::");
        String[] columns = {"CUST_ID", "CUST_NAME", "ACTIVE_EMUI_VERSION", "DOB", "DOJ", "BIGINT_COLUMN1", "BIGINT_COLUMN2", "DECIMAL_COLUMN1", "DECIMAL_COLUMN2", "Double_COLUMN1", "Double_COLUMN2", "INTEGER_COLUMN1"};

        long averageTime = 0;
        for (int iterator = 0; iterator < 3; iterator++) {
            String path = "/integration/flink/target/store/default/100_uniqdata";
            long t1 = System.currentTimeMillis();
            CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
            DataSource dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
            int rowCount = dataSource.collect().size();
            assertEquals(rowCount, 100);
            long t2 = System.currentTimeMillis();
            long timeTaken = t2 - t1;
            LOGGER.info("Time taken : (in milliseconds) " + timeTaken);
            LOGGER.info("Time taken to fetch Hundred records  :  (in milliseconds) " + timeTaken);
            averageTime = averageTime + timeTaken;
            Boolean status = writeToFile("Time taken for Hundred records :::  (in milliseconds) " + timeTaken + "\n");
            assertTrue(status);
        }
        Boolean status = writeToFile("\n" + date.toString() + " : Average Time taken for Hundred records :::  (in milliseconds) " + averageTime / 3 + "\n");
        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");
        assertTrue(status);

        averageTime = 0;
        for (int iterator = 0; iterator < 3; iterator++) {
            LOGGER.info(">>>>>>>>>Thousand records:::::::::::::::::::");
            long t3 = System.currentTimeMillis();
            String path1 = "/integration/flink/target/store/default/1000_uniqdata";
            CarbonDataFlinkInputFormat carbondataFlinkInputFormat1 = new CarbonDataFlinkInputFormat(getRootPath() + path1, columns, false);
            DataSource dataSource1 = env.createInput(carbondataFlinkInputFormat1.getInputFormat());
            int rowCount1 = dataSource1.collect().size();
            assertEquals(rowCount1, 1000);
            long t4 = System.currentTimeMillis();
            long timeTaken1 = t4 - t3;
            LOGGER.info("Time taken to fetch thousand records  :  (in milliseconds) " + timeTaken1);
            averageTime = averageTime + timeTaken1;
            Boolean status1 = writeToFile("Time taken for thousand records :::  (in milliseconds) " + timeTaken1 + "\n");
            assertTrue(status1);
        }
        Boolean status1 = writeToFile("\n" + date.toString() + " : Average Time taken for Hundred records :::  (in milliseconds) " + averageTime / 3 + "\n");
        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");
        assertTrue(status1);


        LOGGER.info(">>>>>>>>>Ten Thousand records:::::::::::::::::::");
        averageTime = 0;
        for (int iterator = 0; iterator < 3; iterator++) {
            long t5 = System.currentTimeMillis();
            String path2 = "/integration/flink/target/store/default/10000_uniqdata";
            CarbonDataFlinkInputFormat carbondataFlinkInputFormat2 = new CarbonDataFlinkInputFormat(getRootPath() + path2, columns, false);
            DataSource dataSource2 = env.createInput(carbondataFlinkInputFormat2.getInputFormat());
            int rowCount2 = dataSource2.collect().size();
            assertEquals(rowCount2, 10000);
            long t6 = System.currentTimeMillis();
            long timeTaken2 = t6 - t5;
            LOGGER.info("Time taken to fetch ten thousand records  :  (in milliseconds) " + timeTaken2);
            averageTime = averageTime + timeTaken2;
            Boolean status2 = writeToFile("Time taken for Ten Thousand records :::  (in milliseconds) " + timeTaken2 + "\n");
            assertTrue(status2);
        }
        Boolean status2 = writeToFile("\n" + date.toString() + " : Average Time taken for Ten Thousand records :::  (in milliseconds) " + averageTime / 3 + "\n");
        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");
        assertTrue(status2);

        LOGGER.info(">>>>>>>>>One Lakh records:::::::::::::::::::");
        averageTime = 0;
        for (int iterator = 0; iterator < 3; iterator++) {
            long t7 = System.currentTimeMillis();
            String path3 = "/integration/flink/target/store/default/1lac_uniqdata";
            CarbonDataFlinkInputFormat carbondataFlinkInputFormat3 = new CarbonDataFlinkInputFormat(getRootPath() + path3, columns, false);
            DataSource dataSource3 = env.createInput(carbondataFlinkInputFormat3.getInputFormat());
            int rowCount3 = dataSource3.collect().size();
            assertEquals(rowCount3, 100000);
            long t8 = System.currentTimeMillis();
            long timeTaken3 = t8 - t7;
            LOGGER.info("Time taken to fetch One Lac records  :  (in milliseconds) " + timeTaken3);
            averageTime = averageTime + timeTaken3;
            Boolean status3 = writeToFile("Time taken for One Lac records :::  (in milliseconds) " + timeTaken3 + "\n");
            assertTrue(status3);
        }
        Boolean status3 = writeToFile("\n" + date.toString() + " : Average Time taken for One Lac records :::  (in milliseconds) " + averageTime / 3 + "\n");
        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");
        assertTrue(status3);

        LOGGER.info(">>>>>>>>>Four Lakh records:::::::::::::::::::");
        averageTime = 0;
        for (int iterator = 0; iterator < 3; iterator++) {
            long t9 = System.currentTimeMillis();
            String path4 = "/integration/flink/target/store/default/4lac_uniqdata";
            CarbonDataFlinkInputFormat carbondataFlinkInputFormat4 = new CarbonDataFlinkInputFormat(getRootPath() + path4, columns, false);
            DataSource dataSource4 = env.createInput(carbondataFlinkInputFormat4.getInputFormat());
            int rowCount4 = dataSource4.collect().size();
            assertEquals(rowCount4, 400000);
            long t10 = System.currentTimeMillis();
            long timeTaken4 = t10 - t9;
            averageTime = averageTime + timeTaken4;
            LOGGER.info("Time taken to fetch Four Lac records  :  (in milliseconds) " + timeTaken4);
            Boolean status4 = writeToFile("Time taken for Four Lac records :::  (in milliseconds) " + timeTaken4 + "\n");
            assertTrue(status4);
        }
        Boolean status4 = writeToFile("\n" + date.toString() + " : Average Time taken for Four Lac records :::  (in milliseconds) " + averageTime / 3 + "\n");
        writeToFile("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n\n");
        assertTrue(status4);
    }

    @AfterClass
    public static void removeStore() throws IOException {
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/carbonmetastore"));
    }
}

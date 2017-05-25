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

import org.apache.carbondata.flink.utils.UnzipUtility;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.operators.DataSource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class CarbonDataFlinkInputFormatTest {

    protected static String[] columns;

    @BeforeClass
    public static void defineStore() throws IOException {
        String zipPath = getRootPath() + "/integration/flink/src/test/resources/store-input.zip";
        String zipDestinationPath = getRootPath() + "/integration/flink/target";

        UnzipUtility unzipUtility = new UnzipUtility();
        unzipUtility.unzip(zipPath, zipDestinationPath);
    }

    static String getRootPath() throws IOException {
        return new File(CarbonFlinkInputFormatPerformanceTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Test
    public void getDataFromCarbon() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        columns = new String[]{"id", "name"};
        String path = "/integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSource<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());

        int rowCount = dataSource.collect().size();
        assert (rowCount == 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromInvalidPath() throws Exception {
        columns = new String[]{"id", "name"};
        String path = "./flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        carbondataFlinkInputFormat.getInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromTableHavingInvalidColumns() throws Exception {
        columns = new String[]{};
        String path = "integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        carbondataFlinkInputFormat.getInputFormat();
    }

    @AfterClass
    public static void removeStore() throws IOException {
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
    }

}

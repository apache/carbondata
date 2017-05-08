package org.apache.carbondata.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.operators.DataSource;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class CarbonDataFlinkInputFormatTest {

    String getRootPath() throws IOException {
        return new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Test
    public void getDataFromCarbon() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"id", "name"};
        String path = "/integration/flink/src/test/resources/store/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSource<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());

        int rowCount = dataSource.collect().size();
        assert (rowCount == 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromInvalidPath() throws Exception {
        String[] columns = {"id", "name"};
        String path = "./flink/src/test/resources/store/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        carbondataFlinkInputFormat.getInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromTableHavingInvalidColumns() throws Exception {
        String[] columns = {};
        String path = "/integration/flink/src/test/resources/store/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        carbondataFlinkInputFormat.getInputFormat();
    }
}

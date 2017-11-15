package org.apache.carbondata.flink.utils;

import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.CarbonContext;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class FlinkTestUtils {

    private final static Logger LOGGER = Logger.getLogger(FlinkTestUtils.class.getName());

    static String getRootPath() throws IOException {
        return new File(FlinkTestUtils.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    String getStoreLocation () throws IOException {
        return getRootPath() + "/integration/flink/target/store";
    }

    public CarbonContext createCarbonContext() throws IOException {
        SparkContext sc = new SparkContext(new SparkConf()
        .setAppName("FLINK_TEST_EXAMPLE")
        .setMaster("local[2]"));
    sc.setLogLevel("ERROR");

    CarbonContext cc = new CarbonContext(sc, getStoreLocation(), getRootPath() + "/integration/flink/target/carbonmetastore");

    CarbonProperties.getInstance().addProperty("carbon.storelocation", getStoreLocation());
    return cc;
    }

    public void createStore(CarbonContext carbonContext, String createTableCommand, String loadTableCommand) throws IOException {
        carbonContext.sql (createTableCommand);
        carbonContext.sql(loadTableCommand);
    }

    public void closeContext(CarbonContext carbonContext) {
        carbonContext.sparkContext().cancelAllJobs();
        carbonContext.sparkContext().stop();
    }
}

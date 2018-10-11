package org.apache.carbondata.examples.sql;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.examples.util.ExampleUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class CarbonSessionExample {

  public static void main(String[] args) {
    File file = new File(CarbonSessionExample.class.getResource("/").getPath() + "../..");
    try {
      String rootPath = file.getCanonicalPath();
      System.setProperty("path.target", rootPath + "/examples/spark2/target");
      PropertyConfigurator.configure(rootPath + "/examples/spark2/src/main/resources/log4j.properties");
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true");
      SparkSession spark = ExampleUtils.createCarbonSession("JavaCarbonSessionExample", 1);
      spark.sparkContext().setLogLevel("INFO");

      spark.sql("DROP TABLE IF EXISTS carbonsession_table");
      spark.sql("DROP TABLE IF EXISTS stored_as_carbondata_table");

      spark.sql(
              "CREATE TABLE carbonsession_table( " +
                      "shortField SHORT, " +
                      "intField INT, " +
                      "bigintField LONG, " +
                      "doubleField DOUBLE, " +
                      "stringField STRING, " +
                      "timestampField TIMESTAMP, " +
                      "decimalField DECIMAL(18,2), " +
                      "dateField DATE, " +
                      "charField CHAR(5), " +
                      "floatField FLOAT " +
                      ") " +
                      "STORED BY 'carbondata' " +
                      "TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')"
      );

      String path = rootPath + "/examples/spark2/src/main/resources/data.csv";
      spark.sql(
              "LOAD DATA LOCAL INPATH " + "\'" + path + "\' " +
                      "INTO TABLE carbonsession_table " +
                      "OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')"
      );

      spark.sql(
              "SELECT charField, stringField, intField " +
                      "FROM carbonsession_table " +
                      "WHERE stringfield = 'spark' AND decimalField > 40"
      ).show();

      spark.sql(
              "SELECT * " +
                      "FROM carbonsession_table WHERE length(stringField) = 5"
      ).show();

      spark.sql(
              "SELECT * " +
                      "FROM carbonsession_table " +
                      "WHERE date_format(dateField, \'yyyy-MM-dd \') = \'2015-07-23\'"
      ).show();

      spark.sql("SELECT count(stringField) FROM carbonsession_table").show();

      spark.sql(
              "SELECT sum(intField), stringField " +
                      "FROM carbonsession_table " +
                      "GROUP BY stringField"
      ).show();

      spark.sql(
              "SELECT t1.*, t2.* " +
                      "FROM carbonsession_table t1, carbonsession_table t2 " +
                      "WHERE t1.stringField = t2.stringField"
      ).show();

      spark.sql(
              "WITH t1 AS ( " +
                      "SELECT * FROM carbonsession_table " +
                      "UNION ALL " +
                      "SELECT * FROM carbonsession_table" +
                      ") " +
                      "SELECT t1.*, t2.* " +
                      "FROM t1, carbonsession_table t2 " +
                      "WHERE t1.stringField = t2.stringField"
      ).show();

      spark.sql(
              "SELECT * " +
                      "FROM carbonsession_table " +
                      "WHERE stringField = 'spark' and floatField > 2.8"
      ).show();

      spark.sql(
              "CREATE TABLE stored_as_carbondata_table( " +
                      "name STRING, " +
                      "age INT" +
                      ") " +
                      "STORED AS carbondata"
      );

      spark.sql("INSERT INTO stored_as_carbondata_table VALUES ('Bob',28) ");
      spark.sql("SELECT * FROM stored_as_carbondata_table").show();

      spark.sql("DROP TABLE IF EXISTS carbonsession_table");
      spark.sql("DROP TABLE IF EXISTS stored_as_carbondata_table");

      spark.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}

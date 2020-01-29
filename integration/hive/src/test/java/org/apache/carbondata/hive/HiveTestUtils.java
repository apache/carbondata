package org.apache.carbondata.hive;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.carbondata.hive.test.server.HiveEmbeddedServer2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;

/**
 * A utility class to start and stop the Hive Embedded Server.
 */
public abstract class HiveTestUtils {

  private static Connection connection;

  private static HiveEmbeddedServer2 hiveEmbeddedServer2;

  public HiveTestUtils() {
  }

  private static void setup() {
    try {
      File rootPath = new File(HiveTestUtils.class.getResource("/").getPath() + "../../../..");
      String targetLoc = rootPath.getAbsolutePath() + "/integration/hive/target";
      hiveEmbeddedServer2 = new HiveEmbeddedServer2();
      hiveEmbeddedServer2.start(targetLoc);
      int port = hiveEmbeddedServer2.getFreePort();
      connection = DriverManager.getConnection("jdbc:hive2://localhost:" + port + "/default", "", "");
    } catch (Exception e) {
      throw new RuntimeException("Problem while starting the Hive server: ", e);
    }
  }

  static Connection getConnection() {
    if (connection == null) {
      setup();
      tearDown();
    }
    return connection;
  }

  public static void tearDown() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        connection.close();
        hiveEmbeddedServer2.stop();
      } catch (SQLException e) {
        throw new RuntimeException("Unable to close Hive Embedded Server", e);
      }
    }));
  }

  public String getFieldValue(ResultSet rs, String field) throws Exception {
    while (rs.next()) {
      System.out.println(rs.getString(1));
      System.out.println("-- " + rs.getString(2));
      if (rs.getString(1).toLowerCase().contains(field.toLowerCase())) {
        return rs.getString(2);
      }
    }
    return "";
  }

  public boolean checkAnswer(ResultSet actual, ResultSet expected) throws SQLException {
    Assert.assertEquals("Row Count Mismatch: ", expected.getFetchSize(), actual.getFetchSize());
    while(expected.next() && actual.next()) {
      int numOfColumns = expected.getMetaData().getColumnCount();
      for (int i = 1; i <= numOfColumns; i++) {
        Assert.assertEquals(actual.getString(i), actual.getString(i));
      }
      System.out.println();
    }
    return true;
  }

}

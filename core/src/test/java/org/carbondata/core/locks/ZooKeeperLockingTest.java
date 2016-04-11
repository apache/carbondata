/**
 * 
 */
package org.carbondata.core.locks;

import java.io.IOException;
import java.util.Properties;

import mockit.NonStrictExpectations;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.carbondata.core.util.CarbonProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ZooKeeperLockingTest {

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    Properties startupProperties = new Properties();
    startupProperties.setProperty("dataDir", "D:/temp");
    startupProperties.setProperty("clientPort", "2181");
    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      quorumConfiguration.parseProperties(startupProperties);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);
    new Thread() {
      public void run() {
        try {
          zooKeeperServer.runFromConfig(configuration);
        } catch (IOException e) {
          System.out.println("ZooKeeper failure");
        }
      }
    }.start();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
      SecurityException {

    final CarbonProperties cp = CarbonProperties.getInstance();
    new NonStrictExpectations(cp) {
      {
        cp.getProperty("/Carbon/locks");
        result = "/carbon/tests";
        cp.getProperty("spark.deploy.zookeeper.url");
        result = "127.0.0.1:2181";
      }
    };

    ZooKeeperLocking zkl = new ZooKeeperLocking(LockType.METADATA_LOCK);
    Assert.assertTrue(zkl.lock());

    ZooKeeperLocking zk2 = new ZooKeeperLocking(LockType.METADATA_LOCK);
    Assert.assertTrue(!zk2.lock());

    Assert.assertTrue(zkl.unlock());
    Assert.assertTrue(zk2.lock());
    Assert.assertTrue(zk2.unlock());
  }

}

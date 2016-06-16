/**
 *
 */
package org.carbondata.core.locks;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

import org.carbondata.core.util.CarbonProperties;

import mockit.NonStrictExpectations;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ZooKeeperLockingTest {

  int freePort;

  /**
   * @throws java.lang.Exception
   */
  @Before public void setUp() throws Exception {
    Properties startupProperties = new Properties();
    startupProperties.setProperty("dataDir", (new File("./target").getAbsolutePath()));
    startupProperties.setProperty("dataLogDir", (new File("./target").getAbsolutePath()));
    freePort = findFreePort();
    startupProperties.setProperty("clientPort", "" + freePort);
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
  @After public void tearDown() throws Exception {
  }

  @Test public void testZooKeeperLockingByTryingToAcquire2Locks()
      throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
      SecurityException {

    final CarbonProperties cp = CarbonProperties.getInstance();
    new NonStrictExpectations(cp) {
      {
        cp.getProperty("/CarbonLocks");
        result = "/carbontests";
        cp.getProperty("spark.deploy.zookeeper.url");
        result = "127.0.0.1:" + freePort;
      }
    };

    ZookeeperInit zki = ZookeeperInit.getInstance("127.0.0.1:" + freePort);

    ZooKeeperLocking zkl = new ZooKeeperLocking(LockUsage.METADATA_LOCK);
    Assert.assertTrue(zkl.lock());

    ZooKeeperLocking zk2 = new ZooKeeperLocking(LockUsage.METADATA_LOCK);
    Assert.assertTrue(!zk2.lock());

    Assert.assertTrue(zkl.unlock());
    Assert.assertTrue(zk2.lock());
    Assert.assertTrue(zk2.unlock());
  }

  /**
   * For finding the free port available.
   *
   * @return
   */
  private static int findFreePort() {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      try {
        socket.close();
      } catch (IOException e) {
        // Ignore IOException on close()
      }
      return port;
    } catch (Exception e) {
      // Ignore
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return 2181;
  }
}

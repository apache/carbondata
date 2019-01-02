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
package org.apache.carbondata.lcm.locks;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.locks.ZooKeeperLocking;
import org.apache.carbondata.core.locks.ZookeeperInit;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.UUID;

/**
 * ZooKeeperLocking Test cases
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

  @Ignore public void testZooKeeperLockingByTryingToAcquire2Locks()
      throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
      SecurityException {

//    final CarbonProperties cp = CarbonProperties.getInstance();
//    new NonStrictExpectations(cp) {
//      {
//        cp.getProperty("/CarbonLocks");
//        result = "/carbontests";
//        cp.getProperty("spark.deploy.zookeeper.url");
//        result = "127.0.0.1:" + freePort;
//      }
//    };

    ZookeeperInit zki = ZookeeperInit.getInstance("127.0.0.1:" + freePort);

    AbsoluteTableIdentifier tableIdentifier = AbsoluteTableIdentifier
        .from(CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
            "dbName", "tableName", UUID.randomUUID().toString());
    ZooKeeperLocking zkl =
        new ZooKeeperLocking(tableIdentifier,
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(zkl.lock());

    ZooKeeperLocking zk2 = new ZooKeeperLocking(
    		tableIdentifier, LockUsage.METADATA_LOCK);
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

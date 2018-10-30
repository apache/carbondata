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

package org.apache.carbondata.core.locks;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * This is a singleton class for initialization of zookeeper client.
 */
public class ZookeeperInit {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ZookeeperInit.class.getName());

  private static ZookeeperInit zooKeeperInit;
  /**
   * zk is the zookeeper client instance
   */
  private ZooKeeper zk;

  private ZookeeperInit(String zooKeeperUrl) {

    int sessionTimeOut = 100000;
    try {
      zk = new ZooKeeper(zooKeeperUrl, sessionTimeOut, new DummyWatcher());

    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }

  }

  public static ZookeeperInit getInstance(String zooKeeperUrl) {

    synchronized (ZookeeperInit.class) {
      if (null == zooKeeperInit) {
        LOGGER.info("Initiating Zookeeper client.");
        zooKeeperInit = new ZookeeperInit(zooKeeperUrl);
      }
    }
    return zooKeeperInit;

  }

  public ZooKeeper getZookeeper() {
    return zk;
  }

  private static class DummyWatcher implements Watcher {
    public void process(WatchedEvent event) {
    }
  }
}

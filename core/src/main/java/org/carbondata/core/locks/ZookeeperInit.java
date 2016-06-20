package org.carbondata.core.locks;

import java.io.IOException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * This is a singleton class for initialization of zookeeper client.
 */
public class ZookeeperInit {

  private static final LogService LOGGER =
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
      LOGGER.error(e.getMessage());
    }

  }

  public static ZookeeperInit getInstance(String zooKeeperUrl) {

    if (null == zooKeeperInit) {
      synchronized (ZookeeperInit.class) {
        if (null == zooKeeperInit) {
          LOGGER.info("Initiating Zookeeper client.");
          zooKeeperInit = new ZookeeperInit(zooKeeperUrl);
        }
      }
    }
    return zooKeeperInit;

  }

  public static ZookeeperInit getInstance() {
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

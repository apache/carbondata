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

import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * For Handling the zookeeper locking implementation
 */
public class ZooKeeperLocking extends AbstractCarbonLock {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ZooKeeperLocking.class.getName());

  /**
   * zk is the zookeeper client instance
   */
  private static ZooKeeper zk;

  /**
   * zooKeeperLocation is the location in the zoo keeper file system where the locks will be
   * maintained.
   */
  private static final String zooKeeperLocation = CarbonCommonConstants.ZOOKEEPER_LOCATION;

  /**
   * Unique folder for each table with DatabaseName_TableName
   */
  private final String tableIdFolder;

  /**
   * lockName is the name of the lock to use. This name should be same for every process that want
   * to share the same lock
   */
  private String lockName;

  /**
   * lockPath is the unique path created for the each instance of the carbon lock.
   */
  private String lockPath;

  private String lockTypeFolder;

  public ZooKeeperLocking(AbsoluteTableIdentifier absoluteTableIdentifier, String lockFile) {
    this(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName()
        + CarbonCommonConstants.FILE_SEPARATOR + absoluteTableIdentifier.getCarbonTableIdentifier()
        .getTableName(), lockFile);
  }

  public static void initialize() {
    String zooKeeperUrl =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ZOOKEEPER_URL);
    if (null == zk) {
      zk = ZookeeperInit.getInstance(zooKeeperUrl).getZookeeper();
    }
  }

  /**
   * @param lockLocation
   * @param lockFile
   */
  public ZooKeeperLocking(String lockLocation, String lockFile) {
    this.lockName = lockFile;
    this.tableIdFolder = zooKeeperLocation + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonTablePath.getLockFilesDirPath(lockLocation);

    initialize();

    this.lockTypeFolder = tableIdFolder + CarbonCommonConstants.FILE_SEPARATOR + lockFile;
    try {
      createBaseNode();
      // if exists returns null then path doesnt exist. so creating.
      if (null == zk.exists(this.tableIdFolder, true)) {
        createRecursivly(this.tableIdFolder);
      }
      // if exists returns null then path doesnt exist. so creating.
      if (null == zk.exists(this.lockTypeFolder, true)) {
        zk.create(this.lockTypeFolder, new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
    }
    initRetry();
  }

  /**
   * Creating a znode in which all the znodes (lock files )are maintained.
   */
  private void createBaseNode() throws KeeperException, InterruptedException {
    if (null == zk.exists(zooKeeperLocation, true)) {
      // creating a znode in which all the znodes (lock files )are maintained.
      zk.create(zooKeeperLocation, new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  /**
   * Create zookeepr node if not exist
   * @param path
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void createRecursivly(String path) throws KeeperException, InterruptedException {
    if (zk.exists(path, true) == null && path.length() > 0) {
      String temp = path.substring(0, path.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
      createRecursivly(temp);
      zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }
  /**
   * Handling of the locking mechanism using zoo keeper.
   */
  @Override public boolean lock() {
    try {
      // create the lock file with lockName.
      lockPath =
          zk.create(this.lockTypeFolder + CarbonCommonConstants.FILE_SEPARATOR + lockName, null,
              Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

      // get the children present in zooKeeperLocation.
      List<String> nodes = zk.getChildren(this.lockTypeFolder, null);

      // sort the childrens
      Collections.sort(nodes);

      // here the logic is , for each lock request zookeeper will create a file ending with
      // incremental digits.
      // so first request will be 00001 next is 00002 and so on.
      // if the current request is 00002 and already one previous request(00001) is present then get
      // children will give both nodes.
      // after the sort we are checking if the lock path is first or not .if it is first then lock
      // has been acquired.

      if (lockPath.endsWith(nodes.get(0))) {
        return true;
      } else {
        // if locking failed then deleting the created lock as next time again new lock file will be
        // created.
        zk.delete(lockPath, -1);
        return false;
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
      return false;
    }
  }

  /**
   * @return status where lock file is unlocked or not.
   */
  @Override public boolean unlock() {
    try {
      // exists will return null if the path doesn't exists.
      if (null != zk.exists(lockPath, true)) {
        zk.delete(lockPath, -1);
        lockPath = null;
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
      return false;
    }
    return true;
  }

}

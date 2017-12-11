package org.apache.carbondata.core.api;

import org.apache.carbondata.core.util.CarbonProperty;

class RuntimeProperty {

  /**
   * hive connection url
   */
  @CarbonProperty
  public static final String HIVE_CONNECTION_URL = "javax.jdo.option.ConnectionURL";

  /**
   * zookeeper url key
   */
  @CarbonProperty
  public static final String ZOOKEEPER_URL = "spark.deploy.zookeeper.url";


  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  @CarbonProperty
  public static final String NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK =
      "carbon.load.metadata.lock.retries";
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  @CarbonProperty
  public static final String MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK ="";


  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  public static final int NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK_DEFAULT = 3;
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  public static final int MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK_DEFAULT = 5;
}

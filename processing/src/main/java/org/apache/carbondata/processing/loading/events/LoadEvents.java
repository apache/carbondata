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

package org.apache.carbondata.processing.loading.events;

import java.util.Map;

import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.spark.sql.SparkSession;

public class LoadEvents {
  /**
   * Class for handling operations before start of a load process.
   * Example usage: For validation purpose
   */
  public static class LoadTablePreExecutionEvent extends Event {
    private CarbonTableIdentifier carbonTableIdentifier;
    private CarbonLoadModel carbonLoadModel;
    private String factPath;
    private boolean isDataFrameDefined;
    private Map<String, String> optionsFinal;
    // userProvidedOptions are needed if we need only the load options given by user
    private Map<String, String> userProvidedOptions;
    private boolean isOverWriteTable;
    private SparkSession sparkSession;

    public LoadTablePreExecutionEvent(CarbonTableIdentifier carbonTableIdentifier,
        CarbonLoadModel carbonLoadModel, String factPath, boolean isDataFrameDefined,
        Map<String, String> optionsFinal, Map<String, String> userProvidedOptions,
        boolean isOverWriteTable, SparkSession sparkSession) {
      this.carbonTableIdentifier = carbonTableIdentifier;
      this.carbonLoadModel = carbonLoadModel;
      this.factPath = factPath;
      this.isDataFrameDefined = isDataFrameDefined;
      this.optionsFinal = optionsFinal;
      this.userProvidedOptions = userProvidedOptions;
      this.isOverWriteTable = isOverWriteTable;
      this.sparkSession = sparkSession;
    }

    public LoadTablePreExecutionEvent(CarbonTableIdentifier carbonTableIdentifier,
        CarbonLoadModel carbonLoadModel) {
      this.carbonTableIdentifier = carbonTableIdentifier;
      this.carbonLoadModel = carbonLoadModel;
    }

    public SparkSession getSparkSession() {
      return sparkSession;
    }

    public CarbonTableIdentifier getCarbonTableIdentifier() {
      return carbonTableIdentifier;
    }

    public CarbonLoadModel getCarbonLoadModel() {
      return carbonLoadModel;
    }

    public String getFactPath() {
      return factPath;
    }

    public boolean isDataFrameDefined() {
      return isDataFrameDefined;
    }

    public Map<String, String> getOptionsFinal() {
      return optionsFinal;
    }

    public Map<String, String> getUserProvidedOptions() {
      return userProvidedOptions;
    }

    public boolean isOverWriteTable() {
      return isOverWriteTable;
    }
  }

  /**
   * Class for handling operations after data load completion and before final
   * commit of load operation. Example usage: For loading MV
   */

  public static class LoadTablePostExecutionEvent extends Event {
    private CarbonTableIdentifier carbonTableIdentifier;
    private CarbonLoadModel carbonLoadModel;

    public LoadTablePostExecutionEvent(CarbonTableIdentifier carbonTableIdentifier,
        CarbonLoadModel carbonLoadModel) {
      this.carbonTableIdentifier = carbonTableIdentifier;
      this.carbonLoadModel = carbonLoadModel;
    }

    public CarbonTableIdentifier getCarbonTableIdentifier() {
      return carbonTableIdentifier;
    }

    public CarbonLoadModel getCarbonLoadModel() {
      return carbonLoadModel;
    }
  }

  /**
   * Event for handling operations after data load completion and before final
   * commit of load operation. Example usage: For loading MV
   */

  public static class LoadTablePreStatusUpdateEvent extends Event {
    private CarbonLoadModel carbonLoadModel;
    private CarbonTableIdentifier carbonTableIdentifier;

    public LoadTablePreStatusUpdateEvent(CarbonTableIdentifier carbonTableIdentifier,
        CarbonLoadModel carbonLoadModel) {
      this.carbonTableIdentifier = carbonTableIdentifier;
      this.carbonLoadModel = carbonLoadModel;
    }

    public CarbonLoadModel getCarbonLoadModel() {
      return carbonLoadModel;
    }

    public CarbonTableIdentifier getCarbonTableIdentifier() {
      return carbonTableIdentifier;
    }
  }

  /**
   * Load Even class will be fired from the Load and compaction class
   * to creating all the load commands for all preaggregate data map
   */
  public static class LoadMetadataEvent extends Event {
    private CarbonTable carbonTable;
    private boolean isCompaction;
    private Map<String, String> options;

    public LoadMetadataEvent(CarbonTable carbonTable, boolean isCompaction,
        Map<String, String> options) {
      this.carbonTable = carbonTable;
      this.isCompaction = isCompaction;
      this.options = options;
    }

    public boolean isCompaction() {
      return isCompaction;
    }

    public CarbonTable getCarbonTable() {
      return carbonTable;
    }

    public Map<String, String> getOptions() {
      return options;
    }
  }

  public static class LoadTablePostStatusUpdateEvent extends Event {
    private CarbonLoadModel carbonLoadModel;

    public LoadTablePostStatusUpdateEvent(CarbonLoadModel carbonLoadModel) {
      this.carbonLoadModel = carbonLoadModel;
    }

    public CarbonLoadModel getCarbonLoadModel() {
      return carbonLoadModel;
    }
  }
}

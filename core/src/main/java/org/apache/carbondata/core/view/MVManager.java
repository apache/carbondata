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

package org.apache.carbondata.core.view;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * It maintains all the mv schemas in it.
 */
@InterfaceAudience.Internal
public abstract class MVManager {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MVManager.class.getName());

  private final MVProvider schemaProvider = new MVProvider();

  private volatile MVCatalog<?> catalog;

  private final Object lock = new Object();

  public MVManager() {

  }

  public abstract List<String> getDatabases();

  public abstract String getDatabaseLocation(String databaseName);

  public boolean hasSchemaOnTable(CarbonTable table) {
    return !table.getMVTablesMap().isEmpty();
  }

  public boolean isMVInSyncWithParentTables(MVSchema mvSchema) throws IOException {
    return schemaProvider.isViewCanBeEnabled(mvSchema, true);
  }

  /**
   * It gives all mv schemas of a given table.
   * For show mv command.
   */
  public List<MVSchema> getSchemasOnTable(CarbonTable table)
      throws IOException {
    return getSchemas(table.getMVTablesMap());
  }

  /**
   * It gives all mv schemas of a given table.
   * For show mv command.
   */
  public List<MVSchema> getSchemasOnTable(String databaseName,
                                          CarbonTable carbonTable) throws IOException {
    return schemaProvider.getSchemas(this, databaseName, carbonTable);
  }

  /**
   * It gives all mv schemas from store.
   */
  public List<MVSchema> getSchemas() throws IOException {
    List<MVSchema> schemas = new ArrayList<>();
    for (String database : this.getDatabases()) {
      try {
        schemas.addAll(this.getSchemas(database));
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        LOGGER.error("Exception Occurred: Skipping MV schemas from database: " + database);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(ex.getMessage());
        }
      }
    }
    return schemas;
  }

  /**
   * It gives all mv schemas from given databases in the store
   */
  public List<MVSchema> getSchemas(Map<String, List<String>> mvTablesMap) throws IOException {
    List<MVSchema> schemas = new ArrayList<>();
    for (Map.Entry<String, List<String>> databaseEntry : mvTablesMap.entrySet()) {
      String database = databaseEntry.getKey();
      List<String> mvTables = databaseEntry.getValue();
      for (String mvTable : mvTables) {
        try {
          schemas.add(this.getSchema(database, mvTable));
        } catch (IOException ex) {
          LOGGER.error("Error while fetching MV schema " + mvTable + " from database: " + database);
          throw ex;
        } catch (Exception ex) {
          LOGGER.error(
              "Exception Occurred: Skipping MV schema " + mvTable + " from database: " + database);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(ex.getMessage());
          }
        }
      }
    }
    return schemas;
  }

  /**
   * It gives all mv schemas from store.
   */
  public List<MVSchema> getSchemas(String databaseName) throws IOException {
    return schemaProvider.getSchemas(this, databaseName);
  }

  public MVSchema getSchema(String databaseName, String viewName) throws IOException {
    return schemaProvider.getSchema(this, databaseName, viewName, false);
  }

  public MVSchema getSchema(String databaseName, String viewName, boolean isRegisterMV)
      throws IOException {
    return schemaProvider.getSchema(this, databaseName, viewName, isRegisterMV);
  }

  /**
   * Saves the mv schema to storage
   *
   * @param viewSchema mv schema
   */
  public void createSchema(String databaseName, MVSchema viewSchema)
      throws IOException {
    schemaProvider.saveSchema(this, databaseName, viewSchema);
  }

  /**
   * Drops the mv schema from storage
   *
   * @param viewName mv name
   */
  public void deleteSchema(String databaseName, String viewName) throws IOException {
    schemaProvider.dropSchema(this, databaseName, viewName);
  }

  /**
   * Get the mv catalog.
   */
  public MVCatalog<?> getCatalog() {
    return catalog;
  }

  /**
   * Get the mv catalog.
   */
  public MVCatalog<?> getCatalog(
      MVCatalogFactory<?> catalogFactory,
      boolean reload) throws IOException {
    MVCatalog<?> catalog = this.catalog;
    if (reload || catalog == null) {
      synchronized (lock) {
        catalog = this.catalog;
        if (reload || catalog == null) {
          catalog = catalogFactory.newCatalog();
          List<MVSchema> schemas = getSchemas();
          if (null == catalog) {
            throw new RuntimeException("Internal Error.");
          }
          for (MVSchema schema : schemas) {
            try {
              catalog.registerSchema(schema);
            } catch (Exception e) {
              // Ignore the schema
              LOGGER.error("Error while registering schema for mv: " + schema.getIdentifier()
                  .getTableName());
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(e.getMessage());
              }
            }
          }
          this.catalog = catalog;
        }
      }
    }
    return catalog;
  }

  /**
   * In case of compaction on mv table,this method will merge the segment list of main table
   * and return updated segment mapping
   *
   * @param mergedLoadName      to find which all segments are merged to new compacted segment
   * @param viewSchema       of mv table
   * @param viewLoadMetadataDetails of mv table
   * @return updated segment map after merging segment list
   */
  @SuppressWarnings("unchecked")
  public static String getUpdatedSegmentMap(String mergedLoadName,
      MVSchema viewSchema,
      LoadMetadataDetails[] viewLoadMetadataDetails) {
    Map<String, List<String>> segmentMapping = new HashMap<>();
    List<RelationIdentifier> relationIdentifiers = viewSchema.getRelatedTables();
    for (RelationIdentifier relationIdentifier : relationIdentifiers) {
      for (LoadMetadataDetails loadMetadataDetail : viewLoadMetadataDetails) {
        if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.COMPACTED) {
          if (mergedLoadName.equalsIgnoreCase(loadMetadataDetail.getMergedLoadName())) {
            Map segmentMap = new Gson().fromJson(loadMetadataDetail.getExtraInfo(), Map.class);
            if (segmentMapping.isEmpty()) {
              segmentMapping.putAll(segmentMap);
            } else {
              segmentMapping.get(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                  + relationIdentifier.getTableName()).addAll(
                  (List<String>) segmentMap.get(
                      relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                          + relationIdentifier.getTableName()));
            }
          }
        }
      }
    }
    Gson gson = new Gson();
    return gson.toJson(segmentMapping);
  }

  /**
   * Get enabled mv status details
   */
  public List<MVStatusDetail> getEnabledStatusDetails() throws IOException {
    List<MVStatusDetail> statusDetails = new ArrayList<>();
    for (String database : this.getDatabases()) {
      try {
        statusDetails.addAll(this.getEnabledStatusDetails(database));
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(ex.getMessage());
        }
      }
    }
    return statusDetails;
  }

  /**
   * Get enabled mv status details
   */
  List<MVStatusDetail> getEnabledStatusDetails(String databaseName)
      throws IOException {
    List<MVStatusDetail> statusDetails = schemaProvider.getStatusDetails(this, databaseName);
    List<MVStatusDetail> enabledStatusDetails = new ArrayList<>(statusDetails.size());
    for (MVStatusDetail statusDetail : statusDetails) {
      if (statusDetail.getStatus() == MVStatus.ENABLED) {
        enabledStatusDetails.add(statusDetail);
      }
    }
    return enabledStatusDetails;
  }

  public void setStatus(RelationIdentifier viewIdentifier, MVStatus viewStatus)
      throws IOException {
    MVSchema schema = getSchema(
        viewIdentifier.getDatabaseName(), viewIdentifier.getTableName());
    if (schema != null) {
      schemaProvider.updateStatus(this, Collections.singletonList(schema), viewStatus);
    }
  }

  public void setStatus(List<MVSchema> viewSchemas, MVStatus viewStatus)
      throws IOException {
    if (viewSchemas != null && !viewSchemas.isEmpty()) {
      schemaProvider.updateStatus(this, viewSchemas, viewStatus);
    }
  }

  public void onDrop(String databaseName, String viewName)
      throws IOException {
    MVSchema viewSchema = getSchema(databaseName, viewName);
    if (viewSchema != null) {
      schemaProvider.updateStatus(
          this, Collections.singletonList(viewSchema), MVStatus.DROPPED);
    }
  }

  /**
   * This method will remove all segments of MV table in case of Insert-Overwrite/Update/Delete
   * operations on main table
   *
   * @param schemas mv schemas
   */
  public void onTruncate(List<MVSchema> schemas)
      throws IOException {
    for (MVSchema schema : schemas) {
      if (!schema.isRefreshOnManual()) {
        setStatus(schema.identifier, MVStatus.DISABLED);
      }
      RelationIdentifier relationIdentifier = schema.getIdentifier();
      SegmentStatusManager segmentStatusManager = new SegmentStatusManager(AbsoluteTableIdentifier
          .from(relationIdentifier.getTablePath(),
              relationIdentifier.getDatabaseName(),
              relationIdentifier.getTableName()));
      ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
      try {
        if (carbonLock.lockWithRetries()) {
          LOGGER.info("Acquired lock for table" + relationIdentifier.getDatabaseName() + "."
              + relationIdentifier.getTableName() + " for table status update");
          String metaDataPath =
              CarbonTablePath.getMetadataPath(relationIdentifier.getTablePath());
          LoadMetadataDetails[] loadMetadataDetails =
              SegmentStatusManager.readLoadMetadata(metaDataPath);
          for (LoadMetadataDetails entry : loadMetadataDetails) {
            entry.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          }
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(relationIdentifier.getTablePath()),
              loadMetadataDetails);
        } else {
          LOGGER.error("Not able to acquire the lock for Table status update for table "
              + relationIdentifier.getDatabaseName() + "." + relationIdentifier
              .getTableName());
        }
      } finally {
        if (carbonLock.unlock()) {
          LOGGER.info(
              "Table unlocked successfully after table status update" + relationIdentifier
                  .getDatabaseName() + "." + relationIdentifier.getTableName());
        } else {
          LOGGER.error(
              "Unable to unlock Table lock for table" + relationIdentifier.getDatabaseName()
                  + "." + relationIdentifier.getTableName()
                  + " during table status update");
        }
      }
    }
  }

}

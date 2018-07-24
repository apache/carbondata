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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DiskBasedDMSchemaStoraheProviderSuite {

  @BeforeClass public static void setUp() throws IOException {
    String path =
        new File(DiskBasedDMSchemaStoraheProviderSuite.class.getResource("/").getPath() + "../")
            .getCanonicalPath().replaceAll("\\\\", "/");

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, path);
    FileFactory.deleteAllCarbonFilesOfDir(
        FileFactory.getCarbonFile(CarbonProperties.getInstance().getSystemFolderLocation()));
  }

  @AfterClass public static void tearDown() {
    FileFactory.deleteAllCarbonFilesOfDir(
        FileFactory.getCarbonFile(CarbonProperties.getInstance().getSystemFolderLocation()));
  }

  private DiskBasedDMSchemaStorageProvider provider = new DiskBasedDMSchemaStorageProvider(
      CarbonProperties.getInstance().getSystemFolderLocation());

  @Test public void testSaveSchema() throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = createDataMapSchema("dm1", "table1");
    provider.saveSchema(dataMapSchema);
    CarbonFile[] schemaFilesFromLocation = getSchemaFilesFromLocation();
    assert (existsSchema(dataMapSchema, schemaFilesFromLocation));
    DataMapSchema dataMapSchema1 = provider.retrieveSchema("dm1");
    assert (dataMapSchema.getDataMapName().equals(dataMapSchema1.getDataMapName()));
  }

  @Test public void testDropSchema() throws IOException {
    DataMapSchema dataMapSchema = createDataMapSchema("dm2", "table1");
    provider.saveSchema(dataMapSchema);
    provider.dropSchema("dm2");
    CarbonFile[] schemaFilesFromLocation = getSchemaFilesFromLocation();
    for (CarbonFile file : schemaFilesFromLocation) {
      assert (!file.getName().contains("dm2"));
    }
    try {
      provider.retrieveSchema("dm2");
      assert (false);
    } catch (NoSuchDataMapException e) {
      // Ignore
    }
  }

  @Test public void testRetriveAllSchemas() throws IOException {
    DataMapSchema dataMapSchema1 = createDataMapSchema("dm3", "table1");
    DataMapSchema dataMapSchema2 = createDataMapSchema("dm4", "table1");
    DataMapSchema dataMapSchema3 = createDataMapSchema("dm5", "table1");
    provider.saveSchema(dataMapSchema1);
    provider.saveSchema(dataMapSchema2);
    provider.saveSchema(dataMapSchema3);

    List<DataMapSchema> dataMapSchemas = provider.retrieveAllSchemas();
    assert (existsSchema(dataMapSchema1, dataMapSchemas));
    assert (existsSchema(dataMapSchema2, dataMapSchemas));
    assert (existsSchema(dataMapSchema3, dataMapSchemas));
  }

  @Test public void testWithOtherProvider() throws IOException, InterruptedException {
    DataMapSchema dataMapSchema1 = createDataMapSchema("dm6", "table1");
    DataMapSchema dataMapSchema2 = createDataMapSchema("dm7", "table1");
    DataMapSchema dataMapSchema3 = createDataMapSchema("dm8", "table1");
    provider.saveSchema(dataMapSchema1);
    Thread.sleep(400);
    provider.saveSchema(dataMapSchema2);
    Thread.sleep(400);
    DiskBasedDMSchemaStorageProvider provider1 = new DiskBasedDMSchemaStorageProvider(
        CarbonProperties.getInstance().getSystemFolderLocation());
    provider1.saveSchema(dataMapSchema3);
    Thread.sleep(400);

    List<DataMapSchema> dataMapSchemas = provider1.retrieveAllSchemas();
    assert (existsSchema(dataMapSchema1, dataMapSchemas));
    assert (existsSchema(dataMapSchema2, dataMapSchemas));
    assert (existsSchema(dataMapSchema3, dataMapSchemas));

    List<DataMapSchema> dataMapSchemas1 = provider.retrieveAllSchemas();
    assert (existsSchema(dataMapSchema1, dataMapSchemas1));
    assert (existsSchema(dataMapSchema2, dataMapSchemas1));
    assert (existsSchema(dataMapSchema3, dataMapSchemas1));
  }

  @Test public void testDropWithOtherProvider() throws IOException, InterruptedException {
    DataMapSchema dataMapSchema1 = createDataMapSchema("dm9", "table1");
    DataMapSchema dataMapSchema2 = createDataMapSchema("dm10", "table1");
    DataMapSchema dataMapSchema3 = createDataMapSchema("dm11", "table1");
    provider.saveSchema(dataMapSchema1);
    Thread.sleep(400);
    provider.saveSchema(dataMapSchema2);
    Thread.sleep(400);
    provider.saveSchema(dataMapSchema3);
    Thread.sleep(400);

    DiskBasedDMSchemaStorageProvider provider1 = new DiskBasedDMSchemaStorageProvider(
        CarbonProperties.getInstance().getSystemFolderLocation());
    provider1.dropSchema(dataMapSchema3.getDataMapName());
    Thread.sleep(400);

    List<DataMapSchema> dataMapSchemas = provider1.retrieveAllSchemas();
    assert (existsSchema(dataMapSchema1, dataMapSchemas));
    assert (existsSchema(dataMapSchema2, dataMapSchemas));
    assert (!existsSchema(dataMapSchema3, dataMapSchemas));

    List<DataMapSchema> dataMapSchemas1 = provider.retrieveAllSchemas();
    assert (existsSchema(dataMapSchema1, dataMapSchemas1));
    assert (existsSchema(dataMapSchema2, dataMapSchemas1));
    assert (!existsSchema(dataMapSchema3, dataMapSchemas1));
  }

  private boolean existsSchema(DataMapSchema schema, List<DataMapSchema> dataMapSchemas) {
    for (DataMapSchema dataMapSchema : dataMapSchemas) {
      if (dataMapSchema.getDataMapName().equals(schema.getDataMapName())) {
        return true;
      }
    }
    return false;
  }

  private boolean existsSchema(DataMapSchema schema, CarbonFile[] carbonFiles) {
    for (CarbonFile dataMapSchema : carbonFiles) {
      if (dataMapSchema.getName().contains(schema.getDataMapName())) {
        return true;
      }
    }
    return false;
  }

  private DataMapSchema createDataMapSchema(String name, String table) {
    DataMapSchema mapSchema = new DataMapSchema(name, "index");
    RelationIdentifier identifier = new RelationIdentifier("default", table, "");

    ArrayList<RelationIdentifier> parentTables = new ArrayList<>();
    parentTables.add(identifier);
    mapSchema.setParentTables(parentTables);
    mapSchema.setRelationIdentifier(identifier);
    return mapSchema;
  }

  private CarbonFile[] getSchemaFilesFromLocation() {
    CarbonFile carbonFile =
        FileFactory.getCarbonFile(CarbonProperties.getInstance().getSystemFolderLocation());
    CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".dmschema");
      }
    });
    return carbonFiles;
  }

}

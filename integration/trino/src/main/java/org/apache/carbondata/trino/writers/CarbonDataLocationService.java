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

package org.apache.carbondata.trino.writers;

import java.util.Optional;

import com.google.inject.Inject;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveLocationService;
import io.trino.plugin.hive.LocationHandle;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

public class CarbonDataLocationService extends HiveLocationService {

  private final HdfsEnvironment hdfsEnvironment;

  @Inject
  public CarbonDataLocationService(HdfsEnvironment hdfsEnvironment) {
    super(hdfsEnvironment);
    this.hdfsEnvironment = hdfsEnvironment;
  }

  @Override
  public LocationHandle forNewTable(SemiTransactionalHiveMetastore metastore,
      ConnectorSession session, String schemaName, String tableName,
      Optional<Path> externalLocation) {
    // TODO: test in cloud scenario in S3/OBS and make it compatible for
    //  cloud scenario
    super.forNewTable(metastore, session, schemaName, tableName, externalLocation);
    HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(session);
    Path targetPath =
        HiveWriteUtils.getTableDefaultLocation(context, metastore, this.hdfsEnvironment, schemaName,
            tableName);
    return new LocationHandle(targetPath, targetPath, false,
        LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY);
  }

  @Override
  public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore,
      ConnectorSession session, Table table) {
    // TODO: test in cloud scenario in S3/OBS and make it compatible for
    //  cloud scenario
    super.forExistingTable(metastore, session, table);
    Path targetPath = new Path(table.getStorage().getLocation());
    return new LocationHandle(targetPath, targetPath, true,
        LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY);
  }
}

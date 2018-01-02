package org.apache.carbondata.store.api;

import java.io.IOException;

import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.store.TableBuilder;

public class CarbonStore {

  private CarbonStore() {}

  public static CarbonStore build() {
    return new CarbonStore();
  }

  public Table createTable(String tableName, TableSchema schema, String tablePath)
      throws IOException {
    return TableBuilder.newInstance()
        .tableName(tableName)
        .tablePath(tablePath)
        .tableSchema(schema)
        .create();
  }
}

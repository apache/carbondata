package org.carbondata.hadoop.util;

import java.io.IOException;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.ThriftReader;

import org.apache.thrift.TBase;

/**
 * TODO: It should be removed after store manager implemetation.
 */
public class SchemaReader {

  public CarbonTable readCarbonTableFromStore(CarbonTablePath carbonTablePath,
      CarbonTableIdentifier tableIdentifier, String storePath) throws IOException {
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    if (FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS)
          || FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS)) {
      String tableName = tableIdentifier.getTableName();

      ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
        public TBase create() {
          return new org.carbondata.format.TableInfo();
        }
      };
      ThriftReader thriftReader =
          new ThriftReader(carbonTablePath.getSchemaFilePath(), createTBase);
      thriftReader.open();
      org.carbondata.format.TableInfo tableInfo =
          (org.carbondata.format.TableInfo) thriftReader.read();
      thriftReader.close();

      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo wrapperTableInfo = schemaConverter
          .fromExternalToWrapperTableInfo(tableInfo, tableIdentifier.getDatabaseName(), tableName,
              storePath);
      wrapperTableInfo.setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath));
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
      return CarbonMetadata.getInstance().getCarbonTable(tableIdentifier.getTableUniqueName());
    }
    return null;
  }
}

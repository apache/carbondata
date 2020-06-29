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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.sdk.file.arrow.ArrowConverter;

import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;
import static org.apache.carbondata.core.util.path.CarbonTablePath.CARBON_DATA_EXT;
import static org.apache.carbondata.core.util.path.CarbonTablePath.INDEX_FILE_EXT;

import org.apache.hadoop.conf.Configuration;

/**
 * Schema reader for carbon files, including carbondata file, carbonindex file, and schema file
 */
public class CarbonSchemaReader {

  /**
   * Read schema file and return the schema
   *
   * @param schemaFilePath complete path including schema file name
   * @return schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInSchemaFile(String schemaFilePath) throws IOException {
    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(schemaFilePath);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    List<ColumnSchema> schemaList = schemaConverter
        .fromExternalToWrapperTableInfo(tableInfo, "", "", "")
        .getFactTable()
        .getListOfColumns();
    return new Schema(schemaList);
  }

  /**
   * get carbondata/carbonindex file in path
   *
   * @param path      carbon file path
   * @param extension carbon file extension
   * @param conf      hadoop configuration support, can set s3a AK,SK,
   *                  end point and other conf with this
   * @return CarbonFile array
   */
  private static CarbonFile[] getCarbonFile(String path, final String extension, Configuration conf)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2996
      throws IOException {
    String dataFilePath = path;
    if (!(dataFilePath.endsWith(extension))) {
      CarbonFile[] carbonFiles = FileFactory
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2999
          .getCarbonFile(path, conf)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(extension);
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new IOException("Carbon file not exists.");
      }
      return carbonFiles;
    } else {
      throw new CarbonDataLoadingException("Please ensure path "
          + path + " end with " + extension);
    }
  }

  /**
   * read schema from path,
   * path can be folder path, carbonindex file path, and carbondata file path
   * and will not check all files schema
   *
   * @param path file/folder path
   * @return schema
   * @throws IOException
   */
  public static Schema readSchema(String path) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2999
    Configuration conf = new Configuration();
    return readSchema(path, false, conf);
  }

  /**
   * Converting carbon schema to arrow schema in byte[],
   * byte[] can be converted back arrowSchema by other arrow interface module like pyspark etc.
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static byte[] getArrowSchemaAsBytes(String path) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3365
    Schema schema = CarbonSchemaReader.readSchema(path).asOriginOrder();
    ArrowConverter arrowConverter = new ArrowConverter(schema, 0);
    final byte[] bytes = arrowConverter.toSerializeArray();
    return bytes;
  }

  /**
   * read schema from path,
   * path can be folder path, carbonindex file path, and carbondata file path
   * and will not check all files schema
   *
   * @param path file/folder path
   * @param conf hadoop configuration support, can set s3a AK,SK,end point and other conf with this
   * @return schema
   * @throws IOException
   */
  public static Schema readSchema(String path, Configuration conf) throws IOException {
    return readSchema(path, false, conf);
  }

  /**
   * read schema from path,
   * path can be folder path, carbonindex file path, and carbondata file path
   * and user can decide whether check all files schema
   *
   * @param path           file/folder path
   * @param validateSchema whether check all files schema
   * @param conf           hadoop configuration support, can set s3a AK,SK,
   *                       end point and other conf with this
   * @return schema
   * @throws IOException
   */
  public static Schema readSchema(String path, boolean validateSchema, Configuration conf)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2999
      throws IOException {
    // Check whether it is transational table reads the schema
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3553
    String schemaFilePath = CarbonTablePath.getSchemaFilePath(path);
    if (FileFactory.getCarbonFile(schemaFilePath, conf).exists()) {
      return readSchemaInSchemaFile(schemaFilePath, conf);
    }
    if (path.endsWith(INDEX_FILE_EXT)) {
      return readSchemaFromIndexFile(path, conf);
    } else if (path.endsWith(CARBON_DATA_EXT)) {
      return readSchemaFromDataFile(path, conf);
    } else if (validateSchema) {
      CarbonFile[] carbonIndexFiles = getCarbonFile(path, INDEX_FILE_EXT, conf);
      Schema schema;
      if (carbonIndexFiles != null && carbonIndexFiles.length != 0) {
        schema = readSchemaFromIndexFile(carbonIndexFiles[0].getAbsolutePath(), conf);
        for (int i = 1; i < carbonIndexFiles.length; i++) {
          Schema schema2 = readSchemaFromIndexFile(carbonIndexFiles[i].getAbsolutePath(), conf);
          if (!schema.equals(schema2)) {
            throw new CarbonDataLoadingException("Schema is different between different files.");
          }
        }
        CarbonFile[] carbonDataFiles = getCarbonFile(path, CARBON_DATA_EXT, conf);
        for (int i = 0; i < carbonDataFiles.length; i++) {
          Schema schema2 = readSchemaFromDataFile(carbonDataFiles[i].getAbsolutePath(), conf);
          if (!schema.equals(schema2)) {
            throw new CarbonDataLoadingException("Schema is different between different files.");
          }
        }
        return schema;
      } else {
        throw new CarbonDataLoadingException("No carbonindex file in this path.");
      }
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3446
      return readSchemaFromFolder(path, conf);
    }
  }

  private static Schema readSchemaInSchemaFile(String schemaFilePath, Configuration conf)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3553
      throws IOException {
    org.apache.carbondata.format.TableInfo tableInfo =
        CarbonUtil.readSchemaFile(schemaFilePath, conf);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    TableSchema factTable =
        schemaConverter.fromExternalToWrapperTableInfo(tableInfo, "", "", "").getFactTable();
    List<ColumnSchema> schemaList = factTable.getListOfColumns();
    return new Schema(schemaList, factTable.getTableProperties());
  }

  /**
   * read schema from path,
   * path can be folder path, carbonindex file path, and carbondata file path
   * and user can decide whether check all files schema
   *
   * @param path           file/folder path
   * @param validateSchema whether check all files schema
   * @return schema
   * @throws IOException
   */
  public static Schema readSchema(String path, boolean validateSchema) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2999
    Configuration conf = new Configuration();
    return readSchema(path, validateSchema, conf);
  }

  /**
   * Read carbondata file and return the schema
   * This interface will be removed,
   * please use readSchema instead of this interface
   *
   * @param dataFilePath carbondata file store path
   * @return Schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInDataFile(String dataFilePath) throws IOException {
    return readSchema(dataFilePath, false);
  }

  /**
   * Read schema from carbondata file
   *
   * @param dataFilePath carbondata file path
   * @param conf         hadoop configuration support, can set s3a AK,SK,
   *                     end point and other conf with this
   * @return carbon data schema
   * @throws IOException
   */
  private static Schema readSchemaFromDataFile(String dataFilePath, Configuration conf)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2999
      throws IOException {
    CarbonHeaderReader reader = new CarbonHeaderReader(dataFilePath, conf);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2982
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<ColumnSchema> schemaList = reader.readSchema();
    for (int i = 0; i < schemaList.size(); i++) {
      ColumnSchema columnSchema = schemaList.get(i);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3815
      if (!(columnSchema.isComplexColumn())) {
        columnSchemaList.add(columnSchema);
      }
    }
    return new Schema(columnSchemaList);
  }

  /**
   * Read carbonindex file and return the schema
   * This interface will be removed,
   * please use readSchema instead of this interface
   *
   * @param indexFilePath complete path including index file name
   * @return schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInIndexFile(String indexFilePath) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2996
    return readSchema(indexFilePath, false);
  }

  public static List<StructField> getChildrenCommon(CarbonTable table, String columnName) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3446
    List<CarbonDimension> list = table.getChildren(columnName);
    List<StructField> structFields = new ArrayList<StructField>();
    for (int i = 0; i < list.size(); i++) {
      CarbonDimension carbonDimension = list.get(i);
      if (DataTypes.isStructType(carbonDimension.getDataType())) {
        structFields.add(getStructChildren(table, carbonDimension.getColName()));
        return structFields;
      } else if (DataTypes.isArrayType(carbonDimension.getDataType())) {
        structFields.add(getArrayChildren(table, carbonDimension.getColName()));
        return structFields;
      } else if (DataTypes.isMapType(carbonDimension.getDataType())) {
        //TODO
      } else {
        ColumnSchema columnSchema = carbonDimension.getColumnSchema();
        structFields.add(new StructField(columnSchema.getColumnName(), columnSchema.getDataType()));
      }
    }
    return structFields;
  }

  public static StructField getStructChildren(CarbonTable table, String columnName) {
    List<StructField> structFields = getChildrenCommon(table, columnName);
    return new StructField(columnName, DataTypes.createStructType(structFields));
  }

  public static StructField getArrayChildren(CarbonTable table, String columnName) {
    List<StructField> structFields = getChildrenCommon(table, columnName);
    return structFields.get(0);
  }

  /**
   * Read schema from carbon file folder path
   *
   * @param folderPath carbon file folder path
   * @param conf       hadoop configuration support, can set s3a AK,SK,
   *                   end point and other conf with this
   * @return carbon data Schema
   * @throws IOException
   */
  private static Schema readSchemaFromFolder(String folderPath, Configuration conf)
    throws IOException {
    String tableName = "UnknownTable" + UUID.randomUUID();
    CarbonTable table = CarbonTable.buildTable(folderPath, tableName, conf);
    List<ColumnSchema> columnSchemaList = table.getTableInfo().getFactTable().getListOfColumns();
    int numOfChildren = 0;
    for (ColumnSchema columnSchema : columnSchemaList) {
      if (!(columnSchema.getColumnName().contains(CarbonCommonConstants.POINT))) {
        numOfChildren++;
      }
    }
    Field[] fields = new Field[numOfChildren];

    int indexOfFields = 0;
    for (ColumnSchema columnSchema : columnSchemaList) {
      if (!columnSchema.getColumnName().contains(CarbonCommonConstants.POINT)) {
        if (DataTypes.isStructType(columnSchema.getDataType())) {
          StructField structField = getStructChildren(table, columnSchema.getColumnName());
          List<StructField> list = new ArrayList<>();
          list.add(structField);
          fields[indexOfFields] = new Field(columnSchema.getColumnName(),
            DataTypes.createStructType(list));
          fields[indexOfFields].setSchemaOrdinal(columnSchema.getSchemaOrdinal());
          indexOfFields++;
        } else if (DataTypes.isArrayType(columnSchema.getDataType())) {
          StructField structField = getArrayChildren(table, columnSchema.getColumnName());
          List<StructField> list = new ArrayList<>();
          list.add(structField);
          fields[indexOfFields] = new Field(columnSchema.getColumnName(), "array", list);
          fields[indexOfFields].setSchemaOrdinal(columnSchema.getSchemaOrdinal());
          indexOfFields++;
        } else if (DataTypes.isMapType(columnSchema.getDataType())) {
          //TODO
        } else {
          fields[indexOfFields] = new Field(columnSchema);
          fields[indexOfFields].setSchemaOrdinal(columnSchema.getSchemaOrdinal());
          indexOfFields++;
        }
      }
    }
    return new Schema(fields);
  }

  /**
   * Read schema from carbonindex file
   *
   * @param indexFilePath carbonindex file path
   * @param conf          hadoop configuration support, can set s3a AK,SK,
   *                      end point and other conf with this
   * @return carbon data Schema
   * @throws IOException
   */
  private static Schema readSchemaFromIndexFile(String indexFilePath, Configuration conf) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3555
    IndexHeader readIndexHeader = SegmentIndexFileStore.readIndexHeader(indexFilePath, conf);
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<org.apache.carbondata.format.ColumnSchema> table_columns =
        readIndexHeader.getTable_columns();
    for (org.apache.carbondata.format.ColumnSchema columnSchema : table_columns) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2982
      if (!(columnSchema.column_name.contains("."))) {
        columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(columnSchema));
      }
    }
    return new Schema(columnSchemaList);
  }

  /**
   * This method return the version details in formatted string by reading from carbondata file
   *
   * @param dataFilePath
   * @return
   * @throws IOException
   */
  public static String getVersionDetails(String dataFilePath) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3025
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2996
    long fileSize =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
        FileFactory.getCarbonFile(dataFilePath).getSize();
    FileReader fileReader = FileFactory.getFileHolder(FileFactory.getFileType(dataFilePath));
    ByteBuffer buffer =
        fileReader.readByteBuffer(FileFactory.getUpdatedFilePath(dataFilePath), fileSize - 8, 8);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3051
    fileReader.finish();
    CarbonFooterReaderV3 footerReader =
        new CarbonFooterReaderV3(dataFilePath, buffer.getLong());
    FileFooter3 footer = footerReader.readFooterVersion3();
    if (null != footer.getExtra_info()) {
      return footer.getExtra_info().get(CarbonCommonConstants.CARBON_WRITTEN_BY_FOOTER_INFO)
          + " in version: " + footer.getExtra_info()
          .get(CarbonCommonConstants.CARBON_WRITTEN_VERSION);
    } else {
      return "Version Details are not found in carbondata file";
    }
  }
}

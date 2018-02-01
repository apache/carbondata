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

package org.apache.carbondata.hadoop.api;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverter;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

// Super class of all carbon format implementation.
// Use one of the newXXX method to create carbon format instance.
public abstract class CarbonInputFormat<T> extends FileInputFormat<Void, T>  {

  protected LogService LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String CARBON_CONVERTER = "mapreduce.input.carboninputformat.converter";
  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  public static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databaseName";
  public static final String TABLE_NAME = "mapreduce.input.carboninputformat.tableName";

  private CarbonTable carbonTable;

  CarbonInputFormat() { }

  public static CarbonTableInputFormat newTableFormat(Configuration conf,
      AbsoluteTableIdentifier identifier) {
    return new CarbonTableInputFormat(conf, identifier);
  }

  public static CarbonStreamInputFormat newStreamFormat(Configuration conf,
      AbsoluteTableIdentifier identifier) {
    return new CarbonStreamInputFormat(conf, identifier);
  }

  public static CarbonFileInputFormat newFileFormat() {
    return new CarbonFileInputFormat();
  }

  /**
   * It sets unresolved filter expression.
   *
   * @param configuration
   * @param filterExpression
   */
  public void setFilterPredicates(Configuration configuration, Expression filterExpression) {
    if (filterExpression == null) {
      return;
    }
    try {
      String filterString = ObjectSerializationUtil.convertObjectToString(filterExpression);
      configuration.set(FILTER_PREDICATE, filterString);
    } catch (Exception e) {
      throw new RuntimeException("Error while setting filter expression to Job", e);
    }
  }

  public Expression getFilterPredicates(Configuration configuration) {
    try {
      String filterExprString = configuration.get(FILTER_PREDICATE);
      if (filterExprString == null) {
        return null;
      }
      Object filter = ObjectSerializationUtil.convertStringToObject(filterExprString);
      return (Expression) filter;
    } catch (IOException e) {
      throw new RuntimeException("Error while reading filter expression", e);
    }
  }

  public void setColumnProjection(Configuration configuration, CarbonProjection projection) {
    if (projection == null || projection.isEmpty()) {
      return;
    }
    String[] allColumns = projection.getAllColumns();
    StringBuilder builder = new StringBuilder();
    for (String column : allColumns) {
      builder.append(column).append(",");
    }
    String columnString = builder.toString();
    columnString = columnString.substring(0, columnString.length() - 1);
    configuration.set(COLUMN_PROJECTION, columnString);
  }

  /**
   * Set projection string (comma separated column names) to configuration
   */
  public void setColumnProjection(Configuration configuration, String projectionString) {
    if (projectionString == null || projectionString.isEmpty()) {
      return;
    }
    configuration.set(COLUMN_PROJECTION, projectionString);
  }

  /**
   * Return column names for projection, return null if projection not set in configuration
   */
  protected static String[] getColumnProjection(Configuration configuration) {
    String projectionString = configuration.get(COLUMN_PROJECTION);
    if (projectionString != null) {
      return projectionString.split(",");
    } else {
      return null;
    }
  }

  /**
   * Get the cached CarbonTable or create it by TableInfo in `configuration`
   */
  CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
    if (carbonTable == null) {
      // carbon table should be created either from deserialized table info (schema saved in
      // hive metastore) or by reading schema in HDFS (schema saved in HDFS)
      TableInfo tableInfo = getTableInfo(configuration);
      CarbonTable carbonTable;
      if (tableInfo != null) {
        carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      } else {
        carbonTable = SchemaReader.readCarbonTableFromStore(
            getAbsoluteTableIdentifier(configuration));
      }
      this.carbonTable = carbonTable;
      return carbonTable;
    } else {
      return this.carbonTable;
    }
  }

  QueryModel createQueryModel(Configuration configuration)
      throws IOException {
    CarbonTable carbonTable = getOrCreateCarbonTable(configuration);
    QueryModel model = carbonTable.createQuery(
        getColumnProjection(configuration),
        getFilterPredicates(configuration));
    model.setConverter(getDataTypeConverter(configuration));
    return model;
  }

  /**
   * Set the data type converter for carbon core module, to avoid data conversion
   * between layers
   */
  public void setDataTypeConverter(Configuration configuration, DataTypeConverter converter)
      throws IOException {
    if (null != converter) {
      configuration.set(CARBON_CONVERTER, ObjectSerializationUtil.convertObjectToString(converter));
    }
  }

  static DataTypeConverter getDataTypeConverter(Configuration configuration)
      throws IOException {
    String converter = configuration.get(CARBON_CONVERTER);
    if (converter == null) {
      return new DataTypeConverterImpl();
    }
    return (DataTypeConverter) ObjectSerializationUtil.convertStringToObject(converter);
  }

  /**
   * Create a new CarbonTable instance by TableInfo in `configuration`
   */
  CarbonTable buildCarbonTable(Configuration configuration) throws IOException {
    TableInfo tableInfo = getTableInfo(configuration);
    return CarbonTable.buildFromTableInfo(tableInfo);
  }

  /**
   * Set the `tableInfo` in `configuration`
   */
  public void setTableInfo(Configuration configuration, TableInfo tableInfo)
      throws IOException {
    if (null != tableInfo) {
      configuration.set(TABLE_INFO, CarbonUtil.encodeToString(tableInfo.serialize()));
    }
  }

  /**
   * Get TableInfo object from `configuration`
   */
  TableInfo getTableInfo(Configuration configuration) throws IOException {
    String tableInfoStr = configuration.get(TABLE_INFO);
    if (tableInfoStr == null) {
      return null;
    } else {
      TableInfo output = new TableInfo();
      output.readFields(new DataInputStream(
          new ByteArrayInputStream(CarbonUtil.decodeStringToBytes(tableInfoStr))));
      return output;
    }
  }

  public static CarbonReadSupport getReadSupport(String readSupportClass) {
    CarbonReadSupport readSupport = null;
    if (readSupportClass != null) {
      try {
        Class<?> myClass = Class.forName(readSupportClass);
        Constructor<?> constructor = myClass.getConstructors()[0];
        Object object = constructor.newInstance();
        if (object instanceof CarbonReadSupport) {
          readSupport = (CarbonReadSupport) object;
        }
      } catch (ClassNotFoundException ex) {
        LogServiceFactory.getLogService().error(ex, "Class " + readSupportClass + "not found");
      } catch (Exception ex) {
        LogServiceFactory.getLogService().error(ex, "Error while creating " + readSupportClass);
      }
    }
    return readSupport;
  }

  public CarbonReadSupport<T> getReadSupport(Configuration configuration) {
    String readSupportClass = configuration.get(CARBON_READ_SUPPORT);
    //By default it uses dictionary decoder read class
    CarbonReadSupport<T> readSupport = null;
    if (readSupportClass != null) {
      try {
        Class<?> myClass = Class.forName(readSupportClass);
        Constructor<?> constructor = myClass.getConstructors()[0];
        Object object = constructor.newInstance();
        if (object instanceof CarbonReadSupport) {
          readSupport = (CarbonReadSupport) object;
        }
      } catch (ClassNotFoundException ex) {
        LOGGER.error(ex, "Class " + readSupportClass + "not found");
      } catch (Exception ex) {
        LOGGER.error(ex, "Error while creating " + readSupportClass);
      }
    } else {
      readSupport = new DictionaryDecodeReadSupport<>();
    }
    return readSupport;
  }

  public void setCarbonReadSupport(Configuration configuration,
      Class<? extends CarbonReadSupport> readSupportClass) {
    if (readSupportClass != null) {
      configuration.set(CARBON_READ_SUPPORT, readSupportClass.getName());
    }
  }

  protected static AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
      throws IOException {
    String tablePath = configuration.get(INPUT_DIR, "");
    try {
      return AbsoluteTableIdentifier.from(
          tablePath, getDatabaseName(configuration), getTableName(configuration));
    } catch (InvalidConfigurationException e) {
      throw new IOException(e);
    }
  }

  static void setDatabaseName(Configuration configuration, String databaseName) {
    if (null != databaseName) {
      configuration.set(DATABASE_NAME, databaseName);
    }
  }

  protected static String getDatabaseName(Configuration configuration)
      throws InvalidConfigurationException {
    String databseName = configuration.get(DATABASE_NAME);
    if (null == databseName) {
      throw new InvalidConfigurationException("Database name is not set.");
    }
    return databseName;
  }

  static void setTableName(Configuration configuration, String tableName) {
    if (null != tableName) {
      configuration.set(TABLE_NAME, tableName);
    }
  }

  protected static String getTableName(Configuration configuration)
      throws InvalidConfigurationException {
    String tableName = configuration.get(TABLE_NAME);
    if (tableName == null) {
      throw new InvalidConfigurationException("Table name is not set");
    }
    return tableName;
  }
}

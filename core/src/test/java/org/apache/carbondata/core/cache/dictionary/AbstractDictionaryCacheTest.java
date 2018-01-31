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
package org.apache.carbondata.core.cache.dictionary;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;

public class AbstractDictionaryCacheTest {

  protected static final String PROPERTY_FILE_NAME = "carbonTest.properties";

  protected CarbonTableIdentifier carbonTableIdentifier;

  protected AbsoluteTableIdentifier identifier;

  protected String databaseName;

  protected String tableName;

  protected String carbonStorePath;

  protected Properties props;

  protected List<String> dataSet1;

  protected List<String> dataSet2;

  protected List<String> dataSet3;

  protected String[] columnIdentifiers;

  /**
   * this method will delete the folders recursively
   *
   * @param f
   */
  protected static void deleteRecursiveSilent(CarbonFile f) {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (CarbonFile c : f.listFiles()) {
          deleteRecursiveSilent(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      return;
    }
  }

  /**
   * prepare the dataset required for running test cases
   */
  protected void prepareDataSet() {
    dataSet1 = Arrays.asList(new String[] { "a", "b", "c" });
    dataSet2 = Arrays.asList(new String[] { "d", "e", "f" });
    dataSet3 = Arrays.asList(new String[] { "b", "c", "a", "d" });
  }

  /**
   * This method will remove the column identifiers from lru cache
   */
  protected void removeKeyFromLRUCache(Cache cacheObject) {
    for (int i = 0; i < columnIdentifiers.length; i++) {
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
          createDictionaryColumnUniqueIdentifier(columnIdentifiers[i]);
      cacheObject.invalidate(dictionaryColumnUniqueIdentifier);
    }
  }

  protected DictionaryColumnUniqueIdentifier createDictionaryColumnUniqueIdentifier(
      String columnId) {
	ColumnIdentifier columnIdentifier = new ColumnIdentifier(columnId, null, DataTypes.STRING);
    return new DictionaryColumnUniqueIdentifier(identifier, columnIdentifier,
        DataTypes.STRING);
  }

  /**
   * this method will delete the store path
   */
  protected void deleteStorePath() {
    FileFactory.FileType fileType = FileFactory.getFileType(this.carbonStorePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(this.carbonStorePath, fileType);
    deleteRecursiveSilent(carbonFile);
  }

  /**
   * write dictionary data
   *
   * @param data
   * @throws IOException
   */
  protected void prepareWriterAndWriteData(List<String> data, String columnId)
      throws IOException {
	ColumnIdentifier columnIdentifier = new ColumnIdentifier(columnId, null, null);
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(identifier, columnIdentifier,
            columnIdentifier.getDataType());
    CarbonDictionaryWriter carbonDictionaryWriter =
        new CarbonDictionaryWriterImpl(dictionaryColumnUniqueIdentifier);
    CarbonUtil.checkAndCreateFolder(CarbonTablePath.getMetadataPath(identifier.getTablePath()));
    List<byte[]> valueList = convertStringListToByteArray(data);
    try {
      carbonDictionaryWriter.write(valueList);
    } finally {
      carbonDictionaryWriter.close();
      carbonDictionaryWriter.commit();
    }
  }

  /**
   * this method will convert list of string to list of byte array
   */
  protected List<byte[]> convertStringListToByteArray(List<String> valueList) {
    List<byte[]> byteArrayList = new ArrayList<>(valueList.size());
    for (String value : valueList) {
      byteArrayList.add(value.getBytes(Charset.defaultCharset()));
    }
    return byteArrayList;
  }

  /**
   * this method will read the property file for required details
   * like dbName, tableName, etc
   */
  protected void init() {
    InputStream in = null;
    props = new Properties();
    try {
      URL url = getClass().getClassLoader().getResource(PROPERTY_FILE_NAME);
      if (null != url){

        URI uri = url.toURI();
        File file = new File(uri);
        in = new FileInputStream(file);
        props.load(in);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } finally {
      CarbonUtil.closeStreams(in);
    }
  }
}

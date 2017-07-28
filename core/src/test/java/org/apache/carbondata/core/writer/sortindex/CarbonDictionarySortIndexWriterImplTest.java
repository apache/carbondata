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
package org.apache.carbondata.core.writer.sortindex;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReaderImpl;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * class contains the unit test cases of the dictionary sort index & sort index inverted writing
 */
public class CarbonDictionarySortIndexWriterImplTest {

  private String storePath;
  private CarbonTableIdentifier carbonTableIdentifier = null;
  private ColumnIdentifier columnIdentifier = null;
  private CarbonDictionaryWriter dictionaryWriter = null;
  private CarbonDictionarySortIndexWriter dictionarySortIndexWriter = null;
  private CarbonDictionarySortIndexReader carbonDictionarySortIndexReader = null;

  @Before public void setUp() throws Exception {
    storePath = "target/carbonStore";
    carbonTableIdentifier =
        new CarbonTableIdentifier("testSchema", "carbon", UUID.randomUUID().toString());
    columnIdentifier = new ColumnIdentifier("Name", null, null);
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnIdentifier, columnIdentifier.getDataType(),
        CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier));
    dictionaryWriter =
        new CarbonDictionaryWriterImpl(storePath, carbonTableIdentifier, dictionaryColumnUniqueIdentifier);
    dictionarySortIndexWriter =
        new CarbonDictionarySortIndexWriterImpl(carbonTableIdentifier, dictionaryColumnUniqueIdentifier, storePath);
    carbonDictionarySortIndexReader =
        new CarbonDictionarySortIndexReaderImpl(carbonTableIdentifier, dictionaryColumnUniqueIdentifier, storePath);
  }

  /**
   * s
   * Method to test the write of sortIndex file.
   *
   * @throws Exception
   */
  @Test public void write() throws Exception {

    String metaFolderPath =
        storePath + File.separator + carbonTableIdentifier.getDatabaseName() + File.separator
            + carbonTableIdentifier.getTableName() + File.separator + "Metadata";
    CarbonUtil.checkAndCreateFolder(metaFolderPath);

    List<int[]> indexList = prepareExpectedData();
    int[] data = indexList.get(0);
    for (int i = 0; i < data.length; i++) {
      dictionaryWriter.write(String.valueOf(data[i]));
    }
    dictionaryWriter.close();
    dictionaryWriter.commit();

    List<Integer> sortIndex = Arrays.asList(ArrayUtils.toObject(indexList.get(0)));
    List<Integer> invertedSortIndex = Arrays.asList(ArrayUtils.toObject(indexList.get(1)));
    dictionarySortIndexWriter.writeSortIndex(sortIndex);
    dictionarySortIndexWriter.writeInvertedSortIndex(invertedSortIndex);
    dictionarySortIndexWriter.close();

    List<Integer> actualSortIndex = carbonDictionarySortIndexReader.readSortIndex();
    List<Integer> actualInvertedSortIndex = carbonDictionarySortIndexReader.readInvertedSortIndex();
    for (int i = 0; i < actualSortIndex.size(); i++) {
      assertEquals(sortIndex.get(i), actualSortIndex.get(i));
      assertEquals(invertedSortIndex.get(i), actualInvertedSortIndex.get(i));
    }

  }

  /**
   * @throws Exception
   */
  @Test public void writingEmptyValue() throws Exception {

    List<Integer> sortIndex = new ArrayList<>();
    List<Integer> invertedSortIndex = new ArrayList<>();
    dictionarySortIndexWriter.writeSortIndex(sortIndex);
    dictionarySortIndexWriter.writeInvertedSortIndex(invertedSortIndex);
    dictionarySortIndexWriter.close();
    List<Integer> actualSortIndex = carbonDictionarySortIndexReader.readSortIndex();
    List<Integer> actualInvertedSortIndex = carbonDictionarySortIndexReader.readInvertedSortIndex();
    for (int i = 0; i < actualSortIndex.size(); i++) {
      assertEquals(sortIndex.get(i), actualSortIndex.get(i));
      assertEquals(invertedSortIndex.get(i), actualInvertedSortIndex.get(i));
    }

  }

  private List<int[]> prepareExpectedData() {
    List<int[]> indexList = new ArrayList<>(2);
    int[] sortIndex = { 0, 3, 2, 4, 1 };
    int[] sortIndexInverted = { 0, 2, 4, 1, 2 };
    indexList.add(0, sortIndex);
    indexList.add(1, sortIndexInverted);
    return indexList;
  }

}
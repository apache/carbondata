/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.common.ext;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.ColumnIdentifier;
import org.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.carbondata.core.reader.CarbonDictionaryReader;
import org.carbondata.core.reader.CarbonDictionaryReaderImpl;
import org.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReaderImpl;
import org.carbondata.core.service.DictionaryService;
import org.carbondata.core.writer.CarbonDictionaryWriter;
import org.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;

/**
 * service to get dictionary reader and writer
 */
public class DictionaryFactory implements DictionaryService {

  private static DictionaryService dictService = new DictionaryFactory();

  /**
   * get dictionary writer
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  @Override public CarbonDictionaryWriter getDictionaryWriter(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath) {
    return new CarbonDictionaryWriterImpl(carbonStorePath, carbonTableIdentifier, columnIdentifier);
  }

  /**
   * get dictionary sort index writer
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  @Override public CarbonDictionarySortIndexWriter getDictionarySortIndexWriter(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath) {
    return new CarbonDictionarySortIndexWriterImpl(carbonTableIdentifier, columnIdentifier,
        carbonStorePath);
  }

  /**
   * get dictionary metadata reader
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  @Override public CarbonDictionaryMetadataReader getDictionaryMetadataReader(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath) {
    return new CarbonDictionaryMetadataReaderImpl(carbonStorePath, carbonTableIdentifier,
        columnIdentifier);
  }

  /**
   * get dictionary reader
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  @Override public CarbonDictionaryReader getDictionaryReader(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath) {
    return new CarbonDictionaryReaderImpl(carbonStorePath, carbonTableIdentifier, columnIdentifier);
  }

  /**
   * get dictionary sort index reader
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  @Override public CarbonDictionarySortIndexReader getDictionarySortIndexReader(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath) {
    return new CarbonDictionarySortIndexReaderImpl(carbonTableIdentifier, columnIdentifier,
        carbonStorePath);
  }

  public static DictionaryService getInstance() {
    return dictService;
  }

}

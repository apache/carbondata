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
package org.carbondata.core.service;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.ColumnIdentifier;
import org.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.carbondata.core.reader.CarbonDictionaryReader;
import org.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.carbondata.core.writer.CarbonDictionaryWriter;
import org.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;

/**
 * Dictionary service to get writer and reader
 */
public interface DictionaryService {

  /**
   * get dictionary writer
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  public CarbonDictionaryWriter getDictionaryWriter(CarbonTableIdentifier carbonTableIdentifier,
      ColumnIdentifier columnIdentifier, String carbonStorePath);

  /**
   * get dictionary sort index writer
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  public CarbonDictionarySortIndexWriter getDictionarySortIndexWriter(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath);

  /**
   * get dictionary metadata reader
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  public CarbonDictionaryMetadataReader getDictionaryMetadataReader(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath);

  /**
   * get dictionary reader
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  public CarbonDictionaryReader getDictionaryReader(CarbonTableIdentifier carbonTableIdentifier,
      ColumnIdentifier columnIdentifier, String carbonStorePath);

  /**
   * get dictionary sort index reader
   *
   * @param carbonTableIdentifier
   * @param columnIdentifier
   * @param carbonStorePath
   * @return
   */
  public CarbonDictionarySortIndexReader getDictionarySortIndexReader(
      CarbonTableIdentifier carbonTableIdentifier, ColumnIdentifier columnIdentifier,
      String carbonStorePath);

}

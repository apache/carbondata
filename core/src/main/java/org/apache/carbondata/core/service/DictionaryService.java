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
package org.apache.carbondata.core.service;

import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.apache.carbondata.core.reader.CarbonDictionaryReader;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;

import org.apache.hadoop.conf.Configuration;

/**
 * Dictionary service to get writer and reader
 */
public interface DictionaryService {

  /**
   * get dictionary writer
   *
   * @param carbonTableIdentifier
   * @param dictionaryColumnUniqueIdentifier
   * @param carbonStorePath
   * @return
   */
  CarbonDictionaryWriter getDictionaryWriter(CarbonTableIdentifier carbonTableIdentifier,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, String carbonStorePath,
      Configuration configuration);

  /**
   * get dictionary sort index writer
   *
   * @param carbonTableIdentifier
   * @param dictionaryColumnUniqueIdentifier
   * @param carbonStorePath
   * @return
   */
  CarbonDictionarySortIndexWriter getDictionarySortIndexWriter(
      CarbonTableIdentifier carbonTableIdentifier,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, String carbonStorePath,
      Configuration configuration);

  /**
   * get dictionary metadata reader
   *
   * @param carbonTableIdentifier
   * @param dictionaryColumnUniqueIdentifier
   * @param carbonStorePath
   * @return
   */
  CarbonDictionaryMetadataReader getDictionaryMetadataReader(
      CarbonTableIdentifier carbonTableIdentifier,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, String carbonStorePath,
      Configuration configuration);

  /**
   * get dictionary reader
   *
   * @param carbonTableIdentifier
   * @param dictionaryColumnUniqueIdentifier
   * @param carbonStorePath
   * @return
   */
  CarbonDictionaryReader getDictionaryReader(CarbonTableIdentifier carbonTableIdentifier,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, String carbonStorePath,
      Configuration configuration);

  /**
   * get dictionary sort index reader
   *
   * @param carbonTableIdentifier
   * @param dictionaryColumnUniqueIdentifier
   * @param carbonStorePath
   * @return
   */
  CarbonDictionarySortIndexReader getDictionarySortIndexReader(
      CarbonTableIdentifier carbonTableIdentifier,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, String carbonStorePath,
      Configuration configuration);

}

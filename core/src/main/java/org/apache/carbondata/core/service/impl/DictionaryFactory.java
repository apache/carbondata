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
package org.apache.carbondata.core.service.impl;

import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.apache.carbondata.core.reader.CarbonDictionaryReader;
import org.apache.carbondata.core.reader.CarbonDictionaryReaderImpl;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReaderImpl;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;

/**
 * service to get dictionary reader and writer
 */
public class DictionaryFactory implements DictionaryService {

  private static DictionaryService dictService = new DictionaryFactory();

  /**
   * get dictionary writer
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return
   */
  @Override public CarbonDictionaryWriter getDictionaryWriter(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    return new CarbonDictionaryWriterImpl(dictionaryColumnUniqueIdentifier);
  }

  /**
   * get dictionary sort index writer
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return
   */
  @Override public CarbonDictionarySortIndexWriter getDictionarySortIndexWriter(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    return new CarbonDictionarySortIndexWriterImpl(dictionaryColumnUniqueIdentifier);
  }

  /**
   * get dictionary metadata reader
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return
   */
  @Override public CarbonDictionaryMetadataReader getDictionaryMetadataReader(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    return new CarbonDictionaryMetadataReaderImpl(dictionaryColumnUniqueIdentifier);
  }

  /**
   * get dictionary reader
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return
   */
  @Override public CarbonDictionaryReader getDictionaryReader(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    return new CarbonDictionaryReaderImpl(dictionaryColumnUniqueIdentifier);
  }

  /**
   * get dictionary sort index reader
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return
   */
  @Override public CarbonDictionarySortIndexReader getDictionarySortIndexReader(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    return new CarbonDictionarySortIndexReaderImpl(dictionaryColumnUniqueIdentifier);
  }

  public static DictionaryService getInstance() {
    return dictService;
  }

}

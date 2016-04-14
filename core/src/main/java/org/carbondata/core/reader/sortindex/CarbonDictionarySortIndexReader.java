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
package org.carbondata.core.reader.sortindex;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for reading the dictionary sort index and sort index inverted
 */
public interface CarbonDictionarySortIndexReader extends Closeable {

    /**
     * method for reading the carbon dictionary sort index data
     * from columns sortIndex file.
     *
     * @return The method return's the list of dictionary sort Index and sort Index reverse
     * @throws IOException In case any I/O error occurs
     */
    public List<Integer> readSortIndex() throws IOException;

    /**
     * method for reading the carbon dictionary inverted sort index data
     * from columns sortIndex file.
     *
     * @return The method return's the list of dictionary inverted sort Index
     * @throws IOException In case any I/O error occurs
     */
    public List<Integer> readInvertedSortIndex() throws IOException;
}

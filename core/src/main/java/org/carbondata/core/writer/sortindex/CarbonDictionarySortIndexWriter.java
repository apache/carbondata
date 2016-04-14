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
package org.carbondata.core.writer.sortindex;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for writing the dictionary sort index and sort index revers data.
 */
public interface CarbonDictionarySortIndexWriter extends Closeable {

    /**
     * The method is used write the dictionary sortIndex data to columns
     * sortedIndex file in thrif format.
     *
     * @param sortIndexList list of sortIndex
     * @throws IOException In Case of any I/O errors occurs.
     */
    public void writeSortIndex(List<Integer> sortIndexList) throws IOException;

    /**
     * The method is used write the dictionary sortIndexInverted data to columns
     * sortedIndex file in thrif format.
     *
     * @param invertedSortIndexList list of  sortIndexInverted
     * @throws IOException In Case of any I/O errors occurs.
     */
    public void writeInvertedSortIndex(List<Integer> invertedSortIndexList) throws IOException;

}

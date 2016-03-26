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

/**
 *
 */
package org.carbondata.query.util;

import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.query.datastorage.streams.DataInputStream;
import org.carbondata.query.datastorage.streams.impl.FileDataInputStream;
import org.carbondata.query.datastorage.streams.impl.HDFSFileDataInputStream;

/**
 * @author R00900208
 */
public final class MolapDataInputStreamFactory {
    private MolapDataInputStreamFactory() {

    }

    public static DataInputStream getDataInputStream(String filesLocation, int mdkeysize,
            int msrCount, boolean hasFactCount, String persistenceFileLocation, String tableName,
            FileType fileType) {
        switch (fileType) {
        case LOCAL:
            return new FileDataInputStream(filesLocation, mdkeysize, msrCount, hasFactCount,
                    persistenceFileLocation, tableName);
        case HDFS:
            return new HDFSFileDataInputStream(filesLocation, mdkeysize, msrCount, hasFactCount,
                    persistenceFileLocation, tableName);
        default:
            return new FileDataInputStream(filesLocation, mdkeysize, msrCount, hasFactCount,
                    persistenceFileLocation, tableName);
        }
    }
}

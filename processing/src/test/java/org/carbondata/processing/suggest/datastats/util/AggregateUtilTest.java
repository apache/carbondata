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

package org.carbondata.processing.suggest.datastats.util;

import java.io.*;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.query.expression.DataType;
import org.junit.Test;

public class AggregateUtilTest {
    @Test
    public void testGetDataType_Double() {

        Type type = Type.DOUBLE;
        DataType dataType = DataStatsUtil.getDataType(type);
        Assert.assertEquals(dataType, DataType.DoubleType);
    }

    @Test
    public void testGetDataType_Long() {
        Type type = Type.LONG;
        DataType dataType = DataStatsUtil.getDataType(type);
        Assert.assertEquals(dataType, DataType.LongType);
    }

    @Test
    public void testGetDataType_Boolean() {
        Type type = Type.BOOLEAN;
        DataType dataType = DataStatsUtil.getDataType(type);
        Assert.assertEquals(dataType, DataType.BooleanType);
    }

    @Test
    public void testGetDataType_Default() {
        Type type = Type.TIMESTAMP;
        DataType dataType = DataStatsUtil.getDataType(type);
        Assert.assertEquals(dataType, DataType.IntegerType);
    }

    @Test
    public void testSerializeObject_InvalidPath_throwIOException() {

        new MockUp<FileFactory>() {

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType,
                    boolean performcheck) throws IOException {
                throw new IOException();
            }

        };
        DataStatsUtil.serializeObject(null, "test", "test");
    }

    @Test
    public void testSerializeObject_InvalidPath_DirectoryCreationFailed() {

        new MockUp<FileFactory>() {

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType,
                    boolean performcheck) throws IOException {
                return false;
            }

            @Mock
            public boolean mkdirs(String filePath, FileFactory.FileType fileType)
                    throws IOException {
                return false;
            }

        };
        DataStatsUtil.createDirectory("test");
    }

    @Test
    public void testSerializeObject_ErrorGettingDataOutputStream() {

        new MockUp<FileFactory>() {

            @Mock
            public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
                    throws IOException {
                throw new IOException();
            }

        };
        DataStatsUtil.serializeObject(null, "test", "test");
    }

    @Test
    public void testSerializeObject_ErrorGettingObjectOutputStream() {

        new MockUp<ObjectOutputStream>() {

            @Mock
            public void writeObject(Object object) throws IOException {
                throw new IOException();
            }

        };
        DataStatsUtil.serializeObject(null, "test", "test");
    }

    @Test
    public void testreadSerializedFile_FileDoesnotExist() {

        new MockUp<FileFactory>() {

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType,
                    boolean performFileCheck) throws IOException {
                throw new IOException();
            }

        };
        Assert.assertNull(DataStatsUtil.readSerializedFile("test"));
    }

    @Test
    public void testreadSerializedFile_ErrorGettingDataOutputStream() {

        new MockUp<FileFactory>() {

            @Mock
            public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType)
                    throws IOException {
                throw new IOException();
            }

        };
        Assert.assertNull(DataStatsUtil.readSerializedFile("test"));
    }

    @Test
    public void testreadSerializedFile_ErrorGettingObjectInputStream() {

        new MockUp<FileFactory>() {

            @Mock
            public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType)
                    throws IOException {
                return new DataInputStream(new FileInputStream(new File("test")));
            }

        };
        new MockUp<ObjectInputStream>() {

            @Mock
            public Object readObject() throws IOException {
                throw new IOException();
            }

        };
        Assert.assertNull(DataStatsUtil.readSerializedFile("test"));
    }

}

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

package org.apache.carbondata.core.datastore;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.Writer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import static junit.framework.TestCase.assertTrue;


public class CompressdFileTest
{
  @BeforeClass public static void setUp() throws Exception  {
    String path = "../core/src/test/resources/testFile";
    String content = "hello world";

    makeGzipFile(path, content);
    makeBzip2File(path, content);

  }

  private static void makeGzipFile (String path, String content) throws Exception {
    path = path + ".gz";
    FileOutputStream output = new FileOutputStream(path);
    try {
      Writer writer = new OutputStreamWriter(new GZIPOutputStream(output),
          "UTF-8");
      try {
        writer.write(content);
      } finally {
        writer.close();
      }
    } finally {
      output.close();
  }
}

  private static void makeBzip2File (String path, String content) throws Exception {
    path = path + ".bz2";
    FileOutputStream output = new FileOutputStream(path);
    try {
      Writer writer = new OutputStreamWriter(new BZip2CompressorOutputStream(output),
          "UTF-8");
      try {
        writer.write(content);
      } finally {
        writer.close();
      }
    } finally {
      output.close();
    }
  }

  @Test public void testReadGzFile() throws Exception {
    assertTrue(readCompressed("../core/src/test/resources/testFile.gz").equals("hello world"));
  }

  @Test public void testReadBzip2File() throws Exception {
    assertTrue(readCompressed("../core/src/test/resources/testFile.bz2").equals("hello world"));
  }

  private static String readCompressed(String path) throws Exception {
      DataInputStream fileReader = null;
      BufferedReader bufferedReader = null;
      String readLine = null;

      try {
        fileReader =
            FileFactory.getDataInputStream(path);
        bufferedReader = new BufferedReader(new InputStreamReader(fileReader,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
        readLine = bufferedReader.readLine();
      } finally {
        if (null != fileReader) {
          fileReader.close();
        }

        if (null != bufferedReader) {
          bufferedReader.close();
        }
      }
      return readLine;
  }

  @AfterClass public static void testCleanUp() {
    new File("../core/src/test/resources/testFile.gz").deleteOnExit();
    new File("../core/src/test/resources/testFile.bz2").deleteOnExit();
  }
}

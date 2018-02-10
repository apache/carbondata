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
package org.apache.carbondata.core.scan.result;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BatchResultTest {
  private static BatchResult batchResult;
  private static List<Object[]> rowsList = new ArrayList(2);

  @BeforeClass public static void setUp() {
    batchResult = new BatchResult();
    rowsList.add(0, new Integer[] { 1, 2 });
    rowsList.add(1, new Integer[] { 3 });
  }

  @Test public void testNext() throws NoSuchElementException {
    BatchResult rows = new BatchResult();
    rows.setRows(rowsList);
    Object[] result = rows.next();
    Assert.assertTrue(result.equals(rowsList.get(0)));
  }

  @Test(expected = NoSuchElementException.class) public void testNextWithNoSuchElementException() {
    BatchResult rows = new BatchResult();
    List emptyList = new ArrayList(2);
    rows.setRows(emptyList);
    rows.next();
  }

  @Test public void testGetRows() {
    new MockUp<BatchResult>() {
      @Mock public void $init() {
        //to be left blank
      }
    };
    BatchResult batchResult = new BatchResult();
    List<Object[]> list = batchResult.getRows();
    assertNull("Number of rows is null", list);
  }

  @Test public void testHasNext() {
    List<Object[]> list = new ArrayList<>();
    list.add(0, new Integer[] { 1, 2 });
    list.add(1, new Integer[] { 1, 2 });
    batchResult.setRows(list);
    boolean result = batchResult.hasNext();
    Assert.assertTrue(result);
  }

  @Test public void testGetRawRow() {
    List<Object[]> list = new ArrayList<>();
    list.add(0, new Integer[] { 1, 2 });
    batchResult.setRows(list);
    Object[] actualValue = batchResult.getRawRow(0);
    Assert.assertTrue(list.get(0) == actualValue);
  }

  @Test public void testGetSize() {
    List<Object[]> list = new ArrayList<>();
    list.add(0, new Integer[] { 1, 2 });
    list.add(1, new Integer[] { 1, 2 });
    batchResult.setRows(list);
    int actualValue = batchResult.getSize();
    int expectedValue = 2;
    assertEquals(expectedValue, actualValue);
  }

}

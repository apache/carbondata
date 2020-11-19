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

package org.apache.carbondata.geo;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * GeoHashUtils Tester.
 */
public class GeoHashUtilsTest {

  /**
   * test normal transform from lon,lat to GeoID,with gridSize 50.
   */
  @Test
  public void testLonLat2GeoId() {
    long geoID = GeoHashUtils.lonLat2GeoID(160000000L, 40000000L, 39.9124, 50);
    TestCase.assertEquals(902594178014L, geoID);
  }

  /**
   * test transform from lon=0,lat=0 to GeoID,with gridSize 50.
   */
  @Test
  public void test00LonLat2GeoId() {
    long geoID = GeoHashUtils.lonLat2GeoID(0, 0, 0, 50);
    TestCase.assertEquals((long) Math.pow(2, 39) | (long) Math.pow(2, 38), geoID);
  }

  /**
   * test transform from minimal lon,lat to GeoID,with gridSize 50.
   */
  @Test
  public void testMinimalLonLat2GeoId() {
    long geoID = GeoHashUtils.lonLat2GeoID(-235751615L, -235751615L, 0, 50);
    TestCase.assertEquals(0, geoID);
  }

  /**
   * test transform from maximum lon,lat to GeoID,with gridSize 50.
   */
  @Test
  public void testMaxLonLat2GeoId() {
    long geoID = GeoHashUtils.lonLat2GeoID(235751615L, 235751615L, 0, 50);
    TestCase.assertEquals((long) Math.pow(2, 40) - 1, geoID);
  }

  /**
   * test normal transform from lon,lat to GeoID,with gridSize 5.
   */
  @Test
  public void testLonLat2GeoIdWithGridSize5() {
    long geoID = GeoHashUtils.lonLat2GeoID(160000000L, 40000000L, 39.9124, 5);
    TestCase.assertEquals(58152885790335L, geoID);
  }

  /**
   * test transform from lon=0,lat=0 to GeoID,with gridSize 5.
   */
  @Test
  public void test00LonLat2GeoIdWithGridSize5() {
    long geoID = GeoHashUtils.lonLat2GeoID(0, 0, 0, 5);
    TestCase.assertEquals((long) Math.pow(2, 45) | (long) Math.pow(2, 44), geoID);
  }

  /**
   * test transform from minimal lon,lat to GeoID,with gridSize 5.
   */
  @Test
  public void testMinimalLonLat2GeoIdWithGridSize5() {
    long geoID = GeoHashUtils.lonLat2GeoID(-188601292L, -188601292L, 0, 5);
    TestCase.assertEquals(0, geoID);
  }

  /**
   * test transform from maximum lon,lat to GeoID,with gridSize 5.
   */
  @Test
  public void testMaxLonLat2GeoIdWithGridSize5() {
    long geoID = GeoHashUtils.lonLat2GeoID(188601292L, 188601292L, 0, 5);
    TestCase.assertEquals((long) (Math.pow(2, 46) - 1), geoID);
  }

  /**
   * test normal transform from lon,lat to grid coordinate,with gridSize 50.
   */
  @Test
  public void testLonLat2ColRow() {
    int[] coordinate = GeoHashUtils.lonLat2ColRow(16000000000L, 4000000000L, 39.9124, 50);
    TestCase.assertEquals(613243, coordinate[0]);
    TestCase.assertEquals(797214, coordinate[1]);
  }

  /**
   * test transform from lon=0,lat=0 to grid coordinate,with gridSize 50.
   */
  @Test
  public void test00LonLat2ColRow() {
    int[] coordinate = GeoHashUtils.lonLat2ColRow(0, 0, 0, 50);
    TestCase.assertEquals(1 << 19, coordinate[0]);
    TestCase.assertEquals(1 << 19, coordinate[1]);
  }

  /**
   * test transform from minimal lon,lat to grid coordinate,with gridSize 50.
   */
  @Test
  public void testMinimalLonLat2ColRow() {
    int[] coordinate = GeoHashUtils.lonLat2ColRow(-23575161504L, -23575161504L, 0, 50);
    TestCase.assertEquals(0, coordinate[0]);
    TestCase.assertEquals(0, coordinate[1]);
  }

  /**
   * test transform from minimal lon,lat to grid coordinate,with gridSize 5.
   */
  @Test
  public void testMinimalLonLat2ColRowWithGridSize5() {
    int[] coordinate = GeoHashUtils.lonLat2ColRow(-18860129203L, -18860129203L, 0, 5);
    TestCase.assertEquals(0, coordinate[0]);
    TestCase.assertEquals(0, coordinate[1]);
  }

  /**
   * test transform from maximum lon,lat to grid coordinate,with gridSize 50.
   */
  @Test
  public void testMaxLonLat2ColRow() {
    int[] coordinate = GeoHashUtils.lonLat2ColRow(23575161504L, 23575161504L, 0, 50);
    TestCase.assertEquals((1 << 20) - 1, coordinate[0]);
    TestCase.assertEquals((1 << 20) - 1, coordinate[1]);
  }

  /**
   * test normal transform from GeoID to grid coordinate.
   */
  @Test
  public void testGeoID2ColRow() {
    int[] coordinate = GeoHashUtils.geoID2ColRow(975929064943L);
    TestCase.assertEquals(880111, coordinate[0]);
    TestCase.assertEquals(613243, coordinate[1]);
  }

  /**
   * test transform from GeoID(lon=0,lat=0) to grid coordinate.
   */
  @Test
  public void test00GeoID2ColRow() {
    int[] coordinate = GeoHashUtils.geoID2ColRow((long) Math.pow(2, 39) | (long) Math.pow(2, 38));
    TestCase.assertEquals(1 << 19, coordinate[0]);
    TestCase.assertEquals(1 << 19, coordinate[1]);
  }

  /**
   * test transform from GeoID(0) to grid coordinate.
   */
  @Test
  public void testMinGeoID2ColRow() {
    int[] coordinate = GeoHashUtils.geoID2ColRow(0);
    TestCase.assertEquals(0, coordinate[0]);
    TestCase.assertEquals(0, coordinate[1]);
  }

  /**
   * test transform from maximum GeoID to grid coordinate.
   */
  @Test
  public void testMaxGeoID2ColRow() {
    int[] coordinate = GeoHashUtils.geoID2ColRow((long) Math.pow(2, 40) - 1);
    TestCase.assertEquals((1 << 20) - 1, coordinate[0]);
    TestCase.assertEquals((1 << 20) - 1, coordinate[1]);
  }

  /**
   * test normal transform from grid coordinate to GeoID.
   */
  @Test
  public void testColRow2GeoID() {
    long geoID = GeoHashUtils.colRow2GeoID(880111, 613243);
    TestCase.assertEquals(975929064943L, geoID);
  }

  /**
   * test transform from center grid coordinate to GeoID.
   */
  @Test
  public void test00ColRow2GeoID() {
    long geoID = GeoHashUtils.colRow2GeoID(1 << 19, 1 << 19);
    TestCase.assertEquals((long) Math.pow(2, 39) | (long) Math.pow(2, 38), geoID);
  }

  /**
   * test transform from minimal grid coordinate to GeoID.
   */
  @Test
  public void testMinColRow2GeoID() {
    long geoID = GeoHashUtils.colRow2GeoID(0, 0);
    TestCase.assertEquals(0, geoID);
  }

  /**
   * test transform from maximum grid coordinate to GeoID.
   */
  @Test
  public void testMaxColRow2GeoID() {
    long geoID = GeoHashUtils.colRow2GeoID((1 << 20) - 1, (1 << 20) - 1);
    TestCase.assertEquals((long) Math.pow(2, 40) - 1, geoID);
  }

  /**
   * test get cut times for gridSize = 50 and origin lat = 0
   */
  @Test
  public void testGetCutCount50() {
    TestCase.assertEquals(20, GeoHashUtils.getCutCount(50, 0));
  }

  /**
   * test get cut times for gridSize = 50 and origin lat = 39.9124
   */
  @Test
  public void testGetCutCount39() {
    TestCase.assertEquals(20, GeoHashUtils.getCutCount(50, 39.9124));
  }

  /**
   * test get cut times for gridSize = 5 and origin lat = 0
   */
  @Test
  public void testGetCutCount5() {
    TestCase.assertEquals(23, GeoHashUtils.getCutCount(5, 0));
  }

  /**
   * test get deltaX for gridSize = 50 and origin = 0
   */
  @Test
  public void testGetDeltaX() {
    TestCase.assertEquals(4.496605206422907E-4, GeoHashUtils.getDeltaX(0, 50));
  }

  /**
   * test get deltaY for gridSize = 50 and origin = 0
   */
  @Test
  public void testGetDeltaY() {
    TestCase.assertEquals(4.496605206422907E-4, GeoHashUtils.getDeltaY( 50));
  }
}

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

package org.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

public class RealTimeDataPropertyReader {
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RealTimeDataPropertyReader.class.getName());
  /**
   * Year Surrogate key Map
   */
  private Map<String, Integer> yearMap;
  /**
   * Month Surrogate key Map
   */
  private Map<String, Integer> monthMap;
  /**
   * Day Surrogate key Map
   */
  private Map<String, Integer> dayMap;

  /**
   * Constructor
   *
   * @param schemandCubeName
   * @param columnAndMemberListaMap
   * @param levelTypeColumnMap
   */
  public RealTimeDataPropertyReader(String schemandCubeName,
      Map<String, Set<String>> columnAndMemberListaMap, Map<String, String> levelTypeColumnMap,
      Map<String, Integer> levelAndCardinalityMap) {
    monthMap = new HashMap<String, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    dayMap = new HashMap<String, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    yearMap = new HashMap<String, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    updateMap(schemandCubeName, columnAndMemberListaMap, levelTypeColumnMap,
        levelAndCardinalityMap);
  }

  public static String getMappedDayMemberVal(Integer memberVal) {
    Days[] values = Days.values();
    for (int i = 0; i < values.length; i++) {
      if (memberVal == values[i].getValue()) {
        return values[i].toString();
      }
    }
    return null;
  }

  public static String getMappedMonthMemberVal(Integer memberVal) {
    Months[] values = Months.values();
    for (int i = 0; i < values.length; i++) {
      if (memberVal == values[i].getValue()) {
        return values[i].toString();
      }
    }
    return null;
  }

  private void updateMap(String schemandCubeName, Map<String, Set<String>> columnAndMemberListaMap,
      Map<String, String> levelTypeColumnMap, Map<String, Integer> levelAndCardinalityMap) {
    File realTimeDataFile = new File(CarbonCommonConstants.CARBON_REALTIMEDATA_FILE);

    FileInputStream fileInputStream = null;

    Properties propFile = new Properties();

    try {
      fileInputStream = new FileInputStream(realTimeDataFile);

      propFile.load(fileInputStream);
    } catch (FileNotFoundException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "RealtimeData file not found.");
      }
    } catch (IOException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "Unable to read RealtimeData file.");
      }
    } finally {
      CarbonUtil.closeStreams(fileInputStream);
    }

    String[] splittedName = schemandCubeName.split("/");

    String preKey = splittedName[0] + '.' + splittedName[1] + '.';
    // Update the month Map
    Set<String> monthLevelData = columnAndMemberListaMap.get(levelTypeColumnMap.get("MONTHS"));
    Integer monthCardinality = levelAndCardinalityMap.get(levelTypeColumnMap.get("MONTHS"));
    if (null != monthLevelData && null != monthCardinality) {
      updateMonthMap(propFile, preKey, monthLevelData, monthCardinality);
    }
    // Update the day Map
    Set<String> daysLevelData = columnAndMemberListaMap.get(levelTypeColumnMap.get("DAYS"));
    Integer daysCardinality = levelAndCardinalityMap.get(levelTypeColumnMap.get("DAYS"));
    if (null != daysLevelData && null != daysCardinality) {
      updateDayMap(propFile, preKey, daysLevelData, daysCardinality);
    }

    // Updatethe Year Map
    Set<String> yearLevelData = columnAndMemberListaMap.get(levelTypeColumnMap.get("YEAR"));
    Integer yearCardinality = levelAndCardinalityMap.get(levelTypeColumnMap.get("YEAR"));
    if (null != yearLevelData && null != yearCardinality) {
      updateYearMap(realTimeDataFile, preKey, yearLevelData, yearCardinality);
    }

  }

  private void updateYearMap(File realTimeDataFile, String preKey, Set<String> set,
      int cardinality) {
    preKey = preKey + "YEAR";

    List<String> yearData = readPropertiesFileAndRrtursYearsList(realTimeDataFile, preKey);

    sortAndUpdateYearMap(yearData, preKey, set, cardinality);

  }

  private void sortAndUpdateYearMap(List<String> yearData, String preKey, Set<String> set,
      int cardinality) {
    Map<Integer, String> localYearMap = new TreeMap<Integer, String>();

    for (String line : yearData) {
      String[] split = line.split("=");
      String keyPart = split[0].trim();
      String valuePart = split[1].trim();

      keyPart = keyPart.substring(keyPart.lastIndexOf('.') + 1, keyPart.length());

      if (localYearMap.get(Integer.parseInt(keyPart)) == null) {
        localYearMap.put(Integer.parseInt(keyPart), valuePart);
      }
    }
    int count = 1;
    String value = null;
    int numberOfValues = 0;
    for (Entry<Integer, String> entry : localYearMap.entrySet()) {
      if (numberOfValues > cardinality) {
        break;
      }
      value = entry.getValue();
      if (set.contains(value)) {
        yearMap.put(value, count++);
        numberOfValues++;
      }
    }
  }

  private List<String> readPropertiesFileAndRrtursYearsList(File realTimeDataFile, String preKey) {
    String line = null;
    BufferedReader bufferedReader = null;
    InputStreamReader inputStreamReader = null;
    List<String> yearsData = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    try {

      inputStreamReader =
          new InputStreamReader(new FileInputStream(realTimeDataFile), Charset.defaultCharset());
      bufferedReader = new BufferedReader(inputStreamReader);
      while ((line = bufferedReader.readLine()) != null) {
        if (line.indexOf(preKey) > -1) {
          yearsData.add(line);
        }

      }

    } catch (FileNotFoundException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "RealtimeData file not found.");
      }
    } catch (IOException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "Unable to read  RealtimeData file.");
      }
    } finally {
      CarbonUtil.closeStreams(bufferedReader, inputStreamReader);

    }

    return yearsData;
  }

  private void updateMonthMap(Properties propFile, String preKey, Set<String> set,
      int cardinality) {
    Months[] monthValues = Months.values();
    int numberOfValues = 0;
    for (int i = 0; i < monthValues.length; i++) {
      if (numberOfValues > cardinality) {
        break;
      }
      String value = propFile.getProperty(preKey + monthValues[i]);
      if (set.contains(value)) {
        updateMonthMap(monthValues[i], value);
        numberOfValues++;
      }
    }
  }

  private void updateMonthMap(Months months, String value) {
    if (null == value) {
      return;
    }

    switch (months) {
      case JAN:
        monthMap.put(value, 1);
        break;

      case FEB:
        monthMap.put(value, 2);
        break;

      case MAR:
        monthMap.put(value, 3);
        break;

      case APR:
        monthMap.put(value, 4);
        break;

      case MAY:
        monthMap.put(value, 5);
        break;

      case JUN:
        monthMap.put(value, 6);
        break;

      case JUL:
        monthMap.put(value, 7);
        break;

      case AUG:
        monthMap.put(value, 8);
        break;

      case SEP:
        monthMap.put(value, 9);
        break;

      case OCT:
        monthMap.put(value, 10);
        break;

      case NOV:
        monthMap.put(value, 11);
        break;

      case DEC:
        monthMap.put(value, 12);
        break;
      default:
        break;
    }

  }

  private void updateDayMap(Properties propFile, String preKey, Set<String> set, int cardinality) {
    Days[] dayValues = Days.values();
    int numberOfValues = 0;
    for (int i = 0; i < dayValues.length; i++) {
      if (numberOfValues > cardinality) {
        break;
      }
      String value = propFile.getProperty(preKey + dayValues[i]);
      if (set.contains(value)) {
        updateDayMap(dayValues[i], value);
        numberOfValues++;
      }
    }

  }

  /**
   * Update the Day Map
   *
   * @param days
   * @param value
   */
  private void updateDayMap(Days days, String value) {

    if (null == value) {
      return;
    }

    switch (days) {
      case ONE:
        dayMap.put(value, 1);
        break;

      case TWO:
        dayMap.put(value, 2);
        break;

      case THREE:
        dayMap.put(value, 3);
        break;

      case FOUR:
        dayMap.put(value, 4);
        break;

      case FIVE:
        dayMap.put(value, 5);
        break;

      case SIX:
        dayMap.put(value, 6);
        break;

      case SEVEN:
        dayMap.put(value, 7);
        break;

      case EIGHT:
        dayMap.put(value, 8);
        break;

      case NINE:
        dayMap.put(value, 9);
        break;

      case TEN:
        dayMap.put(value, 10);
        break;

      case ELEVEN:
        dayMap.put(value, 11);
        break;

      case TWELVE:
        dayMap.put(value, 12);
        break;

      case THIRTEEN:
        dayMap.put(value, 13);
        break;

      case FOURTEEN:
        dayMap.put(value, 14);
        break;

      case FIFTEEN:
        dayMap.put(value, 15);
        break;

      case SIXTEEN:
        dayMap.put(value, 16);
        break;

      case SEVENTEEN:
        dayMap.put(value, 17);
        break;

      case EIGHTEEN:
        dayMap.put(value, 18);
        break;

      case NINTEEN:
        dayMap.put(value, 19);
        break;

      case TWENTY:
        dayMap.put(value, 20);
        break;

      case TEWENTYONE:
        dayMap.put(value, 21);
        break;

      case TWENTYTWO:
        dayMap.put(value, 22);
        break;

      case TWENTYTHREE:
        dayMap.put(value, 23);
        break;

      case TWENTYFOUR:
        dayMap.put(value, 24);
        break;

      case TWENTYFIVE:
        dayMap.put(value, 25);
        break;

      case TWENTYSIX:
        dayMap.put(value, 26);
        break;

      case TWENTYSEVEN:
        dayMap.put(value, 27);
        break;

      case TWENTYEIGHT:
        dayMap.put(value, 28);
        break;

      case TWENTYNINE:
        dayMap.put(value, 29);
        break;

      case THIRTY:
        dayMap.put(value, 30);
        break;

      case THIRTYONE:
        dayMap.put(value, 31);
        break;
      default:
        break;
    }

  }

  /**
   * @return Returns the yearMap.
   */
  public Map<String, Integer> getYearMap() {
    return yearMap;
  }

  /**
   * @return Returns the monthMap.
   */
  public Map<String, Integer> getMonthMap() {
    return monthMap;
  }

  /**
   * @return Returns the dayMap.
   */
  public Map<String, Integer> getDayMap() {
    return dayMap;
  }

  /**
   * Months
   */
  public enum Months {
    //CHECKSTYLE:OFF
    JAN(1), FEB(2), MAR(3), APR(4), MAY(5), JUN(6), JUL(7), AUG(8), SEP(9), OCT(10), NOV(11), DEC(
        12);

    private int value;

    private Months(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }//CHECKSTYLE:ON

  /**
   * Days
   */
  public enum Days {
    //CHECKSTYLE:OFF
    ONE(1), TWO(2), THREE(3), FOUR(4), FIVE(5), SIX(6), SEVEN(7), EIGHT(8), NINE(9), TEN(
        10), ELEVEN(11), TWELVE(12), THIRTEEN(13), FOURTEEN(14), FIFTEEN(15), SIXTEEN(
        16), SEVENTEEN(17), EIGHTEEN(18), NINTEEN(19), TWENTY(20), TEWENTYONE(21), TWENTYTWO(
        22), TWENTYTHREE(23), TWENTYFOUR(24), TWENTYFIVE(25), TWENTYSIX(26), TWENTYSEVEN(
        27), TWENTYEIGHT(28), TWENTYNINE(29), THIRTY(30), THIRTYONE(31);
    //CHECKSTYLE:ON
    /**
     *
     */
    private int value;

    private Days(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

}


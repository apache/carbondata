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

package org.apache.carbondata.store.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.memory.UnsafeSortMemoryManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.store.conf.StoreConf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

public class StoreUtil {

  private static LogService LOGGER = LogServiceFactory.getLogService(StoreUtil.class.getName());

  public static void loadProperties(String filePath, StoreConf conf) {
    InputStream input = null;
    try {
      input = new FileInputStream(filePath);
      Properties prop = new Properties();
      prop.load(input);
      for (Map.Entry<Object, Object> entry : prop.entrySet()) {
        conf.conf(entry.getKey().toString(), entry.getValue().toString());
      }
      LOGGER.audit("loaded properties: " + filePath);
    } catch (IOException ex) {
      LOGGER.error(ex, "Failed to load properties file " + filePath);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          LOGGER.error(e);
        }
      }
    }
  }

  public static void initLog4j(String propertiesFilePath) {
    BasicConfigurator.configure();
    PropertyConfigurator.configure(propertiesFilePath);
  }

  public static byte[] serialize(Object object) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    try {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return baos.toByteArray();
  }

  public static Object deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      return ois.readObject();
    } catch (IOException e) {
      LOGGER.error(e);
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    }
    return null;
  }

  public static void configureCSVInputFormat(Configuration configuration,
      CarbonLoadModel carbonLoadModel) {
    CSVInputFormat.setCommentCharacter(configuration, carbonLoadModel.getCommentChar());
    CSVInputFormat.setCSVDelimiter(configuration, carbonLoadModel.getCsvDelimiter());
    CSVInputFormat.setSkipEmptyLine(configuration, carbonLoadModel.getSkipEmptyLine());
    CSVInputFormat.setEscapeCharacter(configuration, carbonLoadModel.getEscapeChar());
    CSVInputFormat.setMaxColumns(configuration, carbonLoadModel.getMaxColumns());
    CSVInputFormat.setNumberOfColumns(configuration,
        "" + carbonLoadModel.getCsvHeaderColumns().length);

    CSVInputFormat.setHeaderExtractionEnabled(
        configuration,
        carbonLoadModel.getCsvHeader() == null ||
            StringUtils.isEmpty(carbonLoadModel.getCsvHeader()));

    CSVInputFormat.setQuoteCharacter(configuration, carbonLoadModel.getQuoteChar());

    CSVInputFormat.setReadBufferSize(
        configuration,
        CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
            CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
  }

  public static void clearUnsafeMemory(long taskId) {
    UnsafeMemoryManager.INSTANCE.freeMemoryAll(taskId);
    UnsafeSortMemoryManager.INSTANCE.freeMemoryAll(taskId);
  }

}

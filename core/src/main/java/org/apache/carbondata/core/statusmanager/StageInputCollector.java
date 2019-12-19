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

package org.apache.carbondata.core.statusmanager;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import static org.apache.carbondata.core.util.path.CarbonTablePath.SUCCESS_FILE_SUBFIX;

/**
 * Utilities to create input split from stage files
 */
public class StageInputCollector {

  private static Logger LOGGER =
      LogServiceFactory.getLogService(StageInputCollector.class.getCanonicalName());

  /**
   * Collect all stage files and create splits from them.
   * These splits can be included in the queried.
   */
  public static List<InputSplit> createInputSplits(CarbonTable table, Configuration hadoopConf)
      throws ExecutionException, InterruptedException {
    List<CarbonFile> stageInputFiles = new LinkedList<>();
    List<CarbonFile> successFiles = new LinkedList<>();
    collectStageFiles(table, hadoopConf, stageInputFiles, successFiles);
    if (stageInputFiles.size() > 0) {
      int numThreads = Math.min(Math.max(stageInputFiles.size(), 1), 10);
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      return createInputSplits(executorService, stageInputFiles);
    } else {
      return new ArrayList<>(0);
    }
  }

  /**
   * Collect all stage files and matched success files.
   * A stage file without success file will not be collected
   */
  public static void collectStageFiles(CarbonTable table, Configuration hadoopConf,
      List<CarbonFile> stageInputList, List<CarbonFile> successFileList) {
    Objects.requireNonNull(table);
    Objects.requireNonNull(hadoopConf);
    Objects.requireNonNull(stageInputList);
    Objects.requireNonNull(successFileList);
    CarbonFile dir = FileFactory.getCarbonFile(table.getStagePath(), hadoopConf);
    if (dir.exists()) {
      CarbonFile[] allFiles = dir.listFiles();
      Map<String, CarbonFile> map = new HashMap<>();
      Arrays.stream(allFiles).filter(file -> file.getName().endsWith(SUCCESS_FILE_SUBFIX))
          .forEach(file -> map.put(file.getName().substring(0, file.getName().indexOf(".")), file));
      Arrays.stream(allFiles).filter(file -> !file.getName().endsWith(SUCCESS_FILE_SUBFIX))
          .filter(file -> map.containsKey(file.getName())).forEach(carbonFile -> {
        stageInputList.add(carbonFile);
        successFileList.add(map.get(carbonFile.getName()));
      });
    }
  }

  /**
   * Read stage files and create input splits from them
   */
  public static List<InputSplit> createInputSplits(
      ExecutorService executorService,
      List<CarbonFile> stageFiles)
      throws ExecutionException, InterruptedException {
    Objects.requireNonNull(executorService);
    Objects.requireNonNull(stageFiles);
    long startTime = System.currentTimeMillis();
    List<InputSplit> output = Collections.synchronizedList(new ArrayList<>());
    Gson gson = new Gson();
    List<Future<Boolean>> futures = stageFiles.stream().map(stageFile ->
        executorService.submit(() -> {
        String filePath = stageFile.getAbsolutePath();
        DataInputStream stream = null;
        try {
          stream = FileFactory.getDataInputStream(filePath, FileFactory.getFileType(filePath));
          StageInput stageInput = gson.fromJson(new InputStreamReader(stream), StageInput.class);
          output.addAll(stageInput.createSplits());
          return true;
        } catch (IOException e) {
          LOGGER.error("failed to read stage file " + filePath);
          return false;
        } finally {
          IOUtils.closeQuietly(stream);
        }
      })
    ).collect(Collectors.toList());
    for (Future<Boolean> future : futures) {
      future.get();
    }
    LOGGER.info("read stage files taken " + (System.currentTimeMillis() - startTime) + "ms");
    return output;
  }
}
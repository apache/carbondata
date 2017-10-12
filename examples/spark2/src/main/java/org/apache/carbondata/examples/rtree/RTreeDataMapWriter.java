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

package org.apache.carbondata.examples.rtree;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;

import com.google.gson.Gson;

public class RTreeDataMapWriter implements DataMapWriter {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableInfo.class.getName());

  private double[] pageLevelMin, pageLevelMax;

  private double[] blockletLevelMin, blockletLevelMax;

  private ArrayList<BlockletBoundingBox> boundingBoxes;

  private String directoryPath;

  public RTreeDataMapWriter(String directoryPath) {
    this.directoryPath = directoryPath;
  }

  @Override
  public void onBlockStart(String blockId) {
    boundingBoxes = new ArrayList<>();
  }

  @Override
  public void onBlockEnd(String blockId) {
    boundingBoxes.sort(Comparator.comparing(BlockletBoundingBox::getBlockletId));
    String filePath = directoryPath.substring(0, directoryPath.lastIndexOf(File.separator) + 1)
        + blockId + ".rtreeindex";
    BufferedWriter brWriter;
    DataOutputStream dataOutStream;
    try {
      FileFactory.createNewFile(filePath, FileFactory.getFileType(filePath));
      dataOutStream = FileFactory.getDataOutputStream(filePath, FileFactory.getFileType(filePath));
      Gson gsonObjectToWrite = new Gson();
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutStream,
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT));
      brWriter.write(gsonObjectToWrite.toJson(boundingBoxes));
      brWriter.flush();
      brWriter.close();
      dataOutStream.close();
    } catch (IOException e) {
      LOGGER.info("Error in writing index file");
    }
  }

  @Override
  public void onBlockletStart(int blockletId) {
    pageLevelMax = null;
    pageLevelMin = null;
    blockletLevelMax = null;
    blockletLevelMin = null;
  }

  @Override
  public void onBlockletEnd(int blockletId) {
    if (blockletLevelMin == null || blockletLevelMax == null) {
      blockletLevelMin = new double[2];
      blockletLevelMax = new double[2];
      blockletLevelMin[0] = pageLevelMin[0];
      blockletLevelMin[1] = pageLevelMin[1];
      blockletLevelMax[0] = pageLevelMax[0];
      blockletLevelMax[1] = pageLevelMax[1];
    } else {
      blockletLevelMin[0] = Math.min(blockletLevelMin[0], pageLevelMin[0]);
      blockletLevelMin[1] = Math.min(blockletLevelMin[1], pageLevelMin[1]);
      blockletLevelMax[0] = Math.min(blockletLevelMax[0], pageLevelMax[0]);
      blockletLevelMax[1] = Math.min(blockletLevelMax[1], pageLevelMax[1]);
    }
    boundingBoxes.add(
        new BlockletBoundingBox(blockletLevelMin, blockletLevelMax, blockletId, directoryPath));
  }

  @Override
  public void onPageAdded(int blockletId, int pageId, ColumnPage[] pages) {
    assert (pages.length == 2);
    if (pageLevelMin == null || pageLevelMax == null) {
      pageLevelMin = new double[2];
      pageLevelMax = new double[2];

      double x = pages[0].getDouble(0);
      double y = pages[1].getDouble(0);
      pageLevelMin[0] = x;
      pageLevelMin[1] = y;
      pageLevelMax[0] = x;
      pageLevelMax[1] = y;
    }

    for (int rowIndex = 0; rowIndex < pages[0].getPageSize(); rowIndex++) {
      double x = pages[0].getDouble(rowIndex);
      double y = pages[1].getDouble(rowIndex);
      pageLevelMin[0] = Math.min(pageLevelMin[0], x);
      pageLevelMin[1] = Math.min(pageLevelMin[1], y);
      pageLevelMax[0] = Math.max(pageLevelMax[0], x);
      pageLevelMax[1] = Math.max(pageLevelMax[1], y);
    }
  }
}

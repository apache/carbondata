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

package org.apache.carbondata.presto.readers;

import java.io.IOException;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * This class reads the String data and convert it into Slice Block
 */
public class SliceStreamReader extends AbstractStreamReader {

  private boolean isDictionary;

  private SliceArrayBlock dictionarySliceArrayBlock;

  public SliceStreamReader() {
  }

  public SliceStreamReader(boolean isDictionary, SliceArrayBlock dictionarySliceArrayBlock) {
    this.isDictionary = isDictionary;
    this.dictionarySliceArrayBlock = dictionarySliceArrayBlock;
  }

  /**
   * Function to create the Slice Block
   *
   * @param type
   * @return
   * @throws IOException
   */
  public Block readBlock(Type type) throws IOException {
    int numberOfRows;
    BlockBuilder builder;
    if (isVectorReader) {
      numberOfRows = batchSize;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (columnVector != null) {
        if (isDictionary) {
          int[] values = new int[numberOfRows];
          for (int i = 0; i < numberOfRows; i++) {
            if (!columnVector.isNullAt(i)) {
              values[i] = (Integer) columnVector.getData(i);
            }
          }
          return new DictionaryBlock(batchSize, dictionarySliceArrayBlock, values);
        } else {
          for (int i = 0; i < numberOfRows; i++) {
            if (columnVector.isNullAt(i)) {
              builder.appendNull();
            } else {
              type.writeSlice(builder, wrappedBuffer((byte[]) columnVector.getData(i)));
            }
          }
        }
      }
    } else {
      numberOfRows = streamData.length;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      if (streamData != null) {
        for (int i = 0; i < numberOfRows; i++) {
          type.writeSlice(builder, utf8Slice(streamData[i].toString()));
        }
      }
    }

    return builder.build();
  }

}

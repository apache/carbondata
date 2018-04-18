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

package org.apache.carbondata.core.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.cache.dictionary.ColumnDictionaryChunkIterator;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.format.ColumnDictionaryChunk;

import org.apache.thrift.TBase;

/**
 * This class performs the functionality of reading a carbon dictionary file.
 * It implements various overloaded method for read functionality.
 */
public class CarbonDictionaryReaderImpl implements CarbonDictionaryReader {

  /**
   * column name
   */
  protected DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier;

  /**
   * dictionary file path
   */
  protected String columnDictionaryFilePath;

  /**
   * dictionary thrift file reader
   */
  private ThriftReader dictionaryFileReader;

  /**
   * Constructor
   *
   * @param dictionaryColumnUniqueIdentifier      column unique identifier
   */
  public CarbonDictionaryReaderImpl(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    this.dictionaryColumnUniqueIdentifier = dictionaryColumnUniqueIdentifier;
    initFileLocation();
  }

  /**
   * This method should be used when complete dictionary data needs to be read.
   * Applicable scenarios :
   * 1. Global dictionary generation in case of incremental load
   * 2. Reading dictionary file on first time query
   * 3. Loading a dictionary column in memory based on query requirement.
   * This is a case where carbon column cache feature is enabled in which a
   * column dictionary is read if it is present in the query.
   *
   * @return list of byte array. Each byte array is unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  @Override public List<byte[]> read() throws IOException {
    return read(0L);
  }

  /**
   * This method should be used when data has to be read from a given offset.
   * Applicable scenarios :
   * 1. Incremental data load. If column dictionary is already loaded in memory
   * and incremental load is done, then for the new query only new dictionary data
   * has to be read form memory.
   *
   * @param startOffset start offset of dictionary file
   * @return list of byte array. Each byte array is unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  @Override public List<byte[]> read(long startOffset) throws IOException {
    List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks =
        readDictionaryMetadataFile();
    // get the last entry for carbon dictionary meta chunk
    CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk =
        carbonDictionaryColumnMetaChunks.get(carbonDictionaryColumnMetaChunks.size() - 1);
    // end offset till where the dictionary file has to be read
    long endOffset = carbonDictionaryColumnMetaChunk.getEnd_offset();
    List<ColumnDictionaryChunk> columnDictionaryChunks =
        read(carbonDictionaryColumnMetaChunks, startOffset, endOffset);
    return getDictionaryList(columnDictionaryChunks);
  }

  /**
   * This method will be used to read data between given start and end offset.
   * Applicable scenarios:
   * 1. Truncate operation. If there is any inconsistency while writing the dictionary file
   * then we can give the start and end offset till where the data has to be retained.
   *
   * @param startOffset start offset of dictionary file
   * @param endOffset   end offset of dictionary file
   * @return iterator over byte array. Each byte array is unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  @Override public Iterator<byte[]> read(long startOffset, long endOffset) throws IOException {
    List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks =
        readDictionaryMetadataFile();
    List<ColumnDictionaryChunk> columnDictionaryChunks =
        read(carbonDictionaryColumnMetaChunks, startOffset, endOffset);
    return (Iterator<byte[]>) new ColumnDictionaryChunkIterator(columnDictionaryChunks);
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override public void close() throws IOException {
    if (null != dictionaryFileReader) {
      dictionaryFileReader.close();
      dictionaryFileReader = null;
    }
  }

  /**
   * @param carbonDictionaryColumnMetaChunks dictionary meta chunk list
   * @param startOffset                      start offset for dictionary data file
   * @param endOffset                        end offset till where data has
   *                                         to be read from dictionary data file
   * @return list of byte column dictionary values
   * @throws IOException readDictionary file method throws IO exception
   */
  private List<ColumnDictionaryChunk> read(
      List<CarbonDictionaryColumnMetaChunk> carbonDictionaryColumnMetaChunks, long startOffset,
      long endOffset) throws IOException {
    // calculate the number of chunks to be read from dictionary file from start offset
    int dictionaryChunkCountsToBeRead =
        calculateTotalDictionaryChunkCountsToBeRead(carbonDictionaryColumnMetaChunks, startOffset,
            endOffset);
    // open dictionary file thrift reader
    openThriftReader();
    // read the required number of chunks from dictionary file
    return readDictionaryFile(startOffset, dictionaryChunkCountsToBeRead);
  }

  /**
   * This method will put all the dictionary chunks into one list and return that list
   *
   * @param columnDictionaryChunks
   * @return
   */
  private List<byte[]> getDictionaryList(List<ColumnDictionaryChunk> columnDictionaryChunks) {
    int dictionaryListSize = 0;
    for (ColumnDictionaryChunk dictionaryChunk : columnDictionaryChunks) {
      dictionaryListSize = dictionaryListSize + dictionaryChunk.getValues().size();
    }
    // convert byte buffer list to byte array list of dictionary values
    List<byte[]> dictionaryValues = new ArrayList<byte[]>(dictionaryListSize);
    for (ColumnDictionaryChunk dictionaryChunk : columnDictionaryChunks) {
      convertAndFillByteBufferListToByteArrayList(dictionaryValues, dictionaryChunk.getValues());
    }
    return dictionaryValues;
  }

  /**
   * This method will convert and fill list of byte buffer to list of byte array
   *
   * @param dictionaryValues          list of byte array. Each byte array is
   *                                  unique dictionary value
   * @param dictionaryValueBufferList dictionary thrift object which is a list of byte buffer.
   *                                  Each dictionary value is a wrapped in byte buffer before
   *                                  writing to file
   */
  private void convertAndFillByteBufferListToByteArrayList(List<byte[]> dictionaryValues,
      List<ByteBuffer> dictionaryValueBufferList) {
    for (ByteBuffer buffer : dictionaryValueBufferList) {
      int length = buffer.limit();
      byte[] value = new byte[length];
      buffer.get(value, 0, value.length);
      dictionaryValues.add(value);
    }
  }

  /**
   * This method will form the path for dictionary file for a given column
   */
  protected void initFileLocation() {
    this.columnDictionaryFilePath = dictionaryColumnUniqueIdentifier.getDictionaryFilePath();
  }

  /**
   * This method will read the dictionary file and return the list of dictionary thrift object
   *
   * @param dictionaryStartOffset        start offset for dictionary file
   * @param dictionaryChunkCountToBeRead number of dictionary chunks to be read
   * @return list of dictionary chunks
   * @throws IOException setReadOffset method throws I/O exception
   */
  private List<ColumnDictionaryChunk> readDictionaryFile(long dictionaryStartOffset,
      int dictionaryChunkCountToBeRead) throws IOException {
    List<ColumnDictionaryChunk> dictionaryChunks =
        new ArrayList<ColumnDictionaryChunk>(dictionaryChunkCountToBeRead);
    // skip the number of bytes if a start offset is given
    dictionaryFileReader.setReadOffset(dictionaryStartOffset);
    // read till dictionary chunk count
    while (dictionaryFileReader.hasNext()
        && dictionaryChunks.size() != dictionaryChunkCountToBeRead) {
      dictionaryChunks.add((ColumnDictionaryChunk) dictionaryFileReader.read());
    }
    return dictionaryChunks;
  }

  /**
   * This method will read the dictionary metadata file for a given column
   * and calculate the number of chunks to be read from the dictionary file.
   * It will do a strict validation for start and end offset as if the offsets are not
   * exactly matching, because data is written in thrift format, the thrift object
   * will not be retrieved properly
   *
   * @param dictionaryChunkMetaList    list of dictionary chunk metadata
   * @param dictionaryChunkStartOffset start offset for a dictionary chunk
   * @param dictionaryChunkEndOffset   end offset for a dictionary chunk
   * @return
   */
  private int calculateTotalDictionaryChunkCountsToBeRead(
      List<CarbonDictionaryColumnMetaChunk> dictionaryChunkMetaList,
      long dictionaryChunkStartOffset, long dictionaryChunkEndOffset) {
    boolean chunkWithStartOffsetFound = false;
    int dictionaryChunkCount = 0;
    for (CarbonDictionaryColumnMetaChunk metaChunk : dictionaryChunkMetaList) {
      // find the column meta chunk whose start offset value matches
      // with the given dictionary start offset
      if (!chunkWithStartOffsetFound && dictionaryChunkStartOffset == metaChunk.getStart_offset()) {
        chunkWithStartOffsetFound = true;
      }
      // start offset is found then keep adding the chunk count to be read
      if (chunkWithStartOffsetFound) {
        dictionaryChunkCount = dictionaryChunkCount + metaChunk.getChunk_count();
      }
      // when end offset is reached then break the loop
      if (dictionaryChunkEndOffset == metaChunk.getEnd_offset()) {
        break;
      }
    }
    return dictionaryChunkCount;
  }

  /**
   * This method will read dictionary metadata file and return the dictionary meta chunks
   *
   * @return list of dictionary metadata chunks
   * @throws IOException read and close method throws IO exception
   */
  private List<CarbonDictionaryColumnMetaChunk> readDictionaryMetadataFile() throws IOException {
    CarbonDictionaryMetadataReader columnMetadataReaderImpl = getDictionaryMetadataReader();
    List<CarbonDictionaryColumnMetaChunk> dictionaryMetaChunkList = null;
    // read metadata file
    try {
      dictionaryMetaChunkList = columnMetadataReaderImpl.read();
    } finally {
      // close the metadata reader
      columnMetadataReaderImpl.close();
    }
    return dictionaryMetaChunkList;
  }

  /**
   * @return
   */
  protected CarbonDictionaryMetadataReader getDictionaryMetadataReader() {
    return new CarbonDictionaryMetadataReaderImpl(this.dictionaryColumnUniqueIdentifier);
  }

  /**
   * This method will open the dictionary file stream for reading
   *
   * @throws IOException thrift reader open method throws IOException
   */
  private void openThriftReader() throws IOException {
    if (null == dictionaryFileReader) {
      // initialise dictionary file reader which will return dictionary thrift object
      // dictionary thrift object contains a list of byte buffer
      dictionaryFileReader =
          new ThriftReader(this.columnDictionaryFilePath, new ThriftReader.TBaseCreator() {
            @Override public TBase create() {
              return new ColumnDictionaryChunk();
            }
          });
      // Open dictionary file reader
      dictionaryFileReader.open();
    }

  }
}

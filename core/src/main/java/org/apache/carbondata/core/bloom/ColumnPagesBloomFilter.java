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
package org.apache.carbondata.core.bloom;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.format.PageBloomChunk;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

/**
 * Store hash function parameters and bitmaps of each page for a column.
 */
public class ColumnPagesBloomFilter {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(ColumnPagesBloomFilter.class.getName());

  /** The vector size of page bloom filter. */
  private int vectorSize;

  /** The number of hash function to consider. */
  private short nbHash;

  /** Type of hashing function to use. */
  private short hashType;

  /** The hash function used to map a key to several positions in the vector. */
  protected HashFunction hash;

  private List<RoaringBitmap> pageBitmaps;

  private List<ByteBuffer> bloomByteBuffers;

  /**
   * This constructor is used for building page blooms.
   */
  public ColumnPagesBloomFilter() {
    int[] bloomParas = BloomFilterUtil.getPageBloomParameters();
    this.vectorSize = bloomParas[0];
    this.nbHash = (short) bloomParas[1];
    this.hashType = Hash.MURMUR_HASH;
    this.hash = new HashFunction(vectorSize, nbHash, hashType);
  }

  /**
   * This constructor is used for query.
   * Only recover the bitmaps for necessary pages
   */
  public ColumnPagesBloomFilter(PageBloomChunk pageBloomChunk) {
    this.vectorSize = pageBloomChunk.getVector_size();
    this.nbHash = pageBloomChunk.getNum_hash();
    this.hashType = pageBloomChunk.getHash_type();
    this.hash = new HashFunction(vectorSize, nbHash, hashType);
    this.bloomByteBuffers = pageBloomChunk.getPagebloom_list();
  }


  public void addPageBloomFilter(BloomFilter bloomFilter) {
    if (null == pageBitmaps) {
      pageBitmaps = new ArrayList<>();
    }
    try {
      RoaringBitmap bitmap = BloomFilterUtil.convertBloomFilterToRoaringBitmap(bloomFilter);
      pageBitmaps.add(bitmap);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * prune pages based on minmax result bitset
   */
  public BitSet prunePages(byte[][] filterValues, BitSet minMaxBitSet) throws IOException {
    // decode pages which is set to TRUE in minMaxBitSet
    RoaringBitmap[] candidatePageBitmaps = new RoaringBitmap[minMaxBitSet.length()];
    int length = minMaxBitSet.cardinality();
    int nextSetBit = minMaxBitSet.nextSetBit(0);
    for (int i = 0; i < length; ++i) {
      byte[] bloomFilterInBytes = bloomByteBuffers.get(nextSetBit).array();
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bloomFilterInBytes));
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(dis);
      candidatePageBitmaps[nextSetBit] = bitmap;
      nextSetBit = minMaxBitSet.nextSetBit(nextSetBit + 1);
    }

    // check if page is hit in bloom
    BitSet pageBitSet = new BitSet(minMaxBitSet.length());
    for (byte[] filterValue : filterValues) {
      int[] ret = hash.hash(new Key(filterValue)); // only hash once
      for (int pageId = 0; pageId < minMaxBitSet.length(); pageId++) {
        if (pageBitSet.get(pageId) // skip if bloom believed any filter in this page
            || !minMaxBitSet.get(pageId)) { // skip if minmax set this page false
          continue;
        }
        RoaringBitmap bitmap = candidatePageBitmaps[pageId];
        boolean pageNeedCheck = true;
        for (int k : ret) {
          if (!bitmap.contains(k)) {
            pageNeedCheck = false;
            break;
          }
        }
        if (pageNeedCheck) {
          // all bit check pass
          pageBitSet.set(pageId);
        }
      }
    }
    return pageBitSet;
  }

  public PageBloomChunk toPageBloomChunk() {
    PageBloomChunk pageBloomChunk = new PageBloomChunk();
    pageBloomChunk.setVector_size(this.vectorSize);
    pageBloomChunk.setNum_hash(this.nbHash);
    pageBloomChunk.setHash_type(this.hashType);
    List<ByteBuffer> bitmaps = new ArrayList<>();

    try {
      for (RoaringBitmap page : pageBitmaps) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(stream);
        page.serialize(out);
        out.close();
        bitmaps.add(ByteBuffer.wrap(stream.toByteArray()));
      }
      pageBloomChunk.setPagebloom_list(bitmaps);
      return pageBloomChunk;
    } catch (IOException e) {
      LOGGER.debug("Failed while converting to PageBloomChunk. " +
              "Disable page bloom for current blocklet.", e);
      // null can be taken as bloom is disabled
      return null;
    }
  }
}

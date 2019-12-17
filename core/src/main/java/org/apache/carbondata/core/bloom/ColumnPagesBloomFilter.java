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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

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

  /** The number of resulting hashed values. */
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
    this.pageBitmaps = Arrays.asList(new RoaringBitmap[bloomByteBuffers.size()]);
  }

  /**
   * compress BloomFilter of a page and add to holder
   */
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
   * deserialize bitmap of page on demand
   */
  private RoaringBitmap getBitmap(int pageIdx) throws IOException {
    RoaringBitmap bitmap = pageBitmaps.get(pageIdx);
    if (null == bitmap) {
      // get bitmap bytes of page
      byte[] bloomFilterInBytes = bloomByteBuffers.get(pageIdx).array();
      // wrap byte array as input stream
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bloomFilterInBytes));
      dis.close();
      bitmap = new RoaringBitmap();
      // deserialize the bitmap from input stream
      bitmap.deserialize(dis);
      pageBitmaps.set(pageIdx, bitmap);
    }
    return bitmap;
  }

  /**
   * check whether need to scan a single page
   */
  public boolean isScanRequired(byte[][] filterValues, int pageIdx) throws IOException {
    RoaringBitmap bitmap = getBitmap(pageIdx);
    for (byte[] filterValue : filterValues) {
      int[] ret = hash.hash(new Key(filterValue));
      boolean pageNeedCheck = true;
      for (int k : ret) {
        if (!bitmap.contains(k)) {
          // a page must not contain the filter value if any bit check fails
          pageNeedCheck = false;
          break;
        }
      }
      if (pageNeedCheck) {
        // a page needed to be scanned if any filter value hit
        return true;
      }
    }
    return false;
  }


  /**
   * prune pages in blocklet based on minmax result bitset
   *
   * For example, we apply filter `col1 in (5,6)`. And values in column pages are:
   *        Column Page 1: 1,3,5,6,9
   *        Column Page 2: 2,4,7,8,10
   *        Column Page 3: 11,15,20,100
   * So, min max will set the bitSet(minMaxBitSet) as (1,1,0). That means page 1 and page 2
   * need to be scanned and we only deserialize bitmaps of these pages.
   * Suppose bloom filter gives totally correct result here.
   * First we check filter value 5, get the hash results(bit positions) by hash function.
   * Then we check each page.
   * For page 1, bitmap of this page is set for each bit position of value 5,
   * that means bloom believes 5 is in this page and this page needs to be scanned.
   * Flag is marked in another bitSet(pageBitSet) as (1,0,0).
   * For page 2, some bit positions of value 5 is not set in this page's bitmap,
   * so value 5 must not exist in this page, no need to mark the flag.
   * For page 3, it is skipped by minmax(see minMaxBitSet), no need to check it with bloom.
   * Secondly we check filter value 6, same as before, we get the bit positions.
   * For page 1, we already decide to scan this page, no need to check it anymore.
   * For page 2, some bit positions of value 6 is not set in this page's bitmap.
   * For page 3, no need to check as before.
   * Finally, page 1 will be scanned, compare to scan both page 1 and page 2 when only use min-max
   */
  public BitSet prunePages(byte[][] filterValues, BitSet minMaxBitSet) throws IOException {
    // check if page is hit in bloom
    BitSet pageBitSet = new BitSet(minMaxBitSet.length());
    for (byte[] filterValue : filterValues) {
      // only hash once to get which bits need to be checked
      int[] ret = hash.hash(new Key(filterValue));
      for (int pageId = 0; pageId < minMaxBitSet.length(); pageId++) {
        if (pageBitSet.get(pageId) // skip if bloom already believed need to scan this page
            || !minMaxBitSet.get(pageId)) { // skip if minmax set this page false
          continue;
        }
        RoaringBitmap bitmap = getBitmap(pageId);
        boolean pageNeedCheck = true;
        for (int k : ret) {
          if (!bitmap.contains(k)) {
            // a page must not contain the filter value if any bit check fails
            pageNeedCheck = false;
            break;
          }
        }
        if (pageNeedCheck) {
          // all bit check pass, need to scan data of this page later
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

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
package org.apache.carbondata.core.util;

public class StatisticObject {

  private long scanTime;
  
  private long readTime;
  
  private int scanBlockletNumber;
  
  private int scanBlockNumber;
  
  private long resultPrpTime;
  
  private long blockletProcessingTime;
  
  private long timeTakenForSnappyUnCompression;
  
  private long timeTakenForInvertedIndex;
  
  private long timeTakenForRle;
  
  private long timeTakenForValueCompression;
  
  private long timeTakenForAllocatingTheObject;
  
  public long getScanTime() {
    return scanTime;
  }

  public synchronized void setScanTime(long scanTime) {
    this.scanTime += scanTime;
  }

  public long getReadTime() {
    return readTime;
  }

  public synchronized void setReadTime(long readTime) {
    this.readTime += readTime;
  }

  public int getScanBlockletNumber() {
    return scanBlockletNumber;
  }

  public synchronized void setScanBlockletNumber(int scanBlockletNumber) {
    this.scanBlockletNumber += scanBlockletNumber;
  }

  public int getScanBlockNumber() {
    return scanBlockNumber;
  }

  public synchronized void setScanBlockNumber(int scanBlockNumber) {
    this.scanBlockNumber += scanBlockNumber;
  }

  public long getResultPrpTime() {
    return resultPrpTime;
  }

  public synchronized void setResultPrpTime(long resultPrpTime) {
    this.resultPrpTime += resultPrpTime;
  }

  public long getBlockletProcessingTime() {
    return blockletProcessingTime;
  }

  public synchronized void setBlockletProcessingTime(long blockletProcessingTime) {
    this.blockletProcessingTime += blockletProcessingTime;
  }

  public long getTimeTakenForSnappyUnCompression() {
    return timeTakenForSnappyUnCompression;
  }

  public synchronized void setTimeTakenForSnappyUnCompression(
      long timeTakenForSnappyUnCompression) {
    this.timeTakenForSnappyUnCompression += timeTakenForSnappyUnCompression;
  }

  public long getTimeTakenForInvertedIndex() {
    return timeTakenForInvertedIndex;
  }

  public synchronized void setTimeTakenForInvertedIndex(long timeTakenForInvertedIndex) {
    this.timeTakenForInvertedIndex += timeTakenForInvertedIndex;
  }

  public long getTimeTakenForRle() {
    return timeTakenForRle;
  }

  public synchronized void setTimeTakenForRle(long timeTakenForRle) {
    this.timeTakenForRle += timeTakenForRle;
  }

  public long getTimeTakenForValueCompression() {
    return timeTakenForValueCompression;
  }

  public synchronized void setTimeTakenForValueCompression(long timeTakenForValueCompression) {
    this.timeTakenForValueCompression += timeTakenForValueCompression;
  }

  public long getTimeTakenForAllocatingTheObject() {
    return timeTakenForAllocatingTheObject;
  }

  public synchronized void setTimeTakenForAllocatingTheObject(
      long timeTakenForAllocatingTheObject) {
    this.timeTakenForAllocatingTheObject += timeTakenForAllocatingTheObject;
  }
}

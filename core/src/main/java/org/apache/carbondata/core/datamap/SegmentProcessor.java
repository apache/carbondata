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

package org.apache.carbondata.core.datamap;

import java.io.Serializable;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentProcessor implements Serializable {
  private static ConcurrentHashMap<String, HashSet<SegmentWorkStatus>> globalWorkQueue =
          new ConcurrentHashMap<String, HashSet<SegmentWorkStatus>>();

  private static SegmentProcessor segmentProcessor = new SegmentProcessor();

  private SegmentProcessor() {
  }

  public static SegmentProcessor getInstance() {
    return segmentProcessor;
  }

  public boolean ifProcessSegment(String segmentNo, String tableID) {
    // return true if global queue does not have instance of that SegmentWorkStatus
    // Syncronize globalWorkQueue
    synchronized (globalWorkQueue) {
      if (globalWorkQueue.containsKey(segmentNo + "_" + tableID)) {
        HashSet<SegmentWorkStatus> lists = globalWorkQueue.get(segmentNo + "_" + tableID);
        // if the getWaiting is false then return true, else return false.
        for (SegmentWorkStatus i : lists) {
          // checking if any segmentworkstatus already exists in the HashSet
          // if any instance of getWaiting is true. Set the current one as true too.
          if (i.getWaiting()) {
            return false;
          }
        }
      }
      return true;
    }
  }

  public void processSegment(SegmentWorkStatus segmentWorkStatus, String tableID) {
    String segmentNumber = segmentWorkStatus.getSegment().getSegmentNo();
    if (globalWorkQueue.containsKey(segmentNumber + "_" + tableID)) {
      HashSet<SegmentWorkStatus> lists = globalWorkQueue.get(segmentNumber + "_" + tableID);
      for (SegmentWorkStatus i : lists) {
        // check if the segmentWorkStatus already exists in the hashset. If it already exists
        // set the necessary waiting status of that object.
        if (i.equals(segmentWorkStatus)) {
          i.setWaiting(segmentWorkStatus.getWaiting());
          return;
        }
      }
      // if the segment work status does not exist in the hashset, add it
      globalWorkQueue.get(segmentNumber + "_" + tableID).add(segmentWorkStatus);
    } else {
      // The first instance of a segment goes here
      HashSet<SegmentWorkStatus> lists = new HashSet<SegmentWorkStatus>();
      lists.add(segmentWorkStatus);
      globalWorkQueue.put(segmentNumber + "_" + tableID, lists);
    }
  }

  public ConcurrentHashMap<String, HashSet<SegmentWorkStatus>> getGlobalWorkQueue() {
    return globalWorkQueue;
  }

  public void emptyQueue() {
    globalWorkQueue.clear();
  }

  // This method updates the Waiting status of the segmentworkstatus that has just been processed
  public void updateWaitingStatus(SegmentWorkStatus prevSegmentWorkStatus, String tableID) {
    String segmentNumber = prevSegmentWorkStatus.getSegment().getSegmentNo();
    if (globalWorkQueue.containsKey(segmentNumber + "_" + tableID)) {
      HashSet<SegmentWorkStatus> lists = globalWorkQueue.get(segmentNumber + "_" + tableID);
      //Update the waiting status of the previous processed segment as false.
      // That specific thread has completed it's processing
      if (lists != null) {
        lists.remove(prevSegmentWorkStatus);
        prevSegmentWorkStatus.setWaiting(false);
        lists.add(prevSegmentWorkStatus);
        // checking for any true instance of getwaiting in the segmentWorkStatus hashset
        for (SegmentWorkStatus i: lists) {
          if (i.getWaiting()) {
            return;
          }
        }
        // delete list from globalworkqueue if no instance in that hashset is true
        // i.e., the segment has been processed completely.
        globalWorkQueue.remove(segmentNumber + "_" + tableID);
      }
    }
  }

}

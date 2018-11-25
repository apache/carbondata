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

package org.apache.carbondata.datamap.minmax;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.cache.Cacheable;

public class MinMaxDataMapCacheKeyValue {

  public static class Key implements Serializable {
    private static final long serialVersionUID = -147823808435277L;
    private String shardPath;

    public Key(String shardPath) {
      this.shardPath = shardPath;
    }

    public String getShardPath() {
      return shardPath;
    }

    @Override
    public String toString() {
      final StringBuffer sb = new StringBuffer("Key{");
      sb.append("shardPath='").append(shardPath).append('\'');
      sb.append('}');
      return sb.toString();
    }

    public String uniqueString() {
      return "minmaxcache_" + shardPath;
    }
  }

  public static class Value implements Cacheable {
    private List<MinMaxIndexHolder> minMaxIndexHolders;
    private int size;

    public Value(List<MinMaxIndexHolder> minMaxIndexHolders) {
      this.minMaxIndexHolders = minMaxIndexHolders;
      for (MinMaxIndexHolder model : minMaxIndexHolders) {
        this.size += model.getSize();
      }
    }

    public List<MinMaxIndexHolder> getMinMaxIndexHolders() {
      return minMaxIndexHolders;
    }

    @Override
    public long getFileTimeStamp() {
      return 0;
    }

    @Override
    public int getAccessCount() {
      return 0;
    }

    @Override
    public long getMemorySize() {
      return size;
    }

    @Override public void invalidate() {
      minMaxIndexHolders = null;
    }
  }
}

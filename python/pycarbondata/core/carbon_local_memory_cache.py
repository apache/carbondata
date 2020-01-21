# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import division

from petastorm.cache import CacheBase


class LocalMemoryCache(CacheBase):
  def __init__(self, size_limit_bytes):
    """LocalMemoryCache is an adapter to a diskcache implementation.

    LocalMemoryCache can be used by a pycarbon Reader class to temporarily keep parts of the dataset in local memory.

    :param size_limit_bytes: Maximal size of the memory to be used by cache. The size of the cache may actually
                             grow somewhat above the size_limit_bytes, so the limit is not very strict.
    """

    self._cache = dict()

  def get(self, key, fill_cache_func):
    value = self._cache.get(key)
    if value is None:
      value = fill_cache_func()
      self._cache[key] = value

    return value

  def cleanup(self):
    self._cache.clear()

  def size(self):
    return len(self._cache)

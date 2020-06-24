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

package org.apache.carbondata.processing.merger;

/**
 * This enum is used to define the types of Compaction.
 * We have 3 types. one is Minor another is Major and
 * finally a compaction done after UPDATE-DELETE operation
 * called IUD compaction.
 */
public enum CompactionType {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1586
    MINOR,
    MAJOR,
    IUD_UPDDEL_DELTA,
    IUD_DELETE_DELTA,
    STREAMING,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1904
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1905
    CLOSE_STREAMING,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2033
    CUSTOM,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2714
    SEGMENT_INDEX,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3440
    UPGRADE_SEGMENT,
    NONE
}

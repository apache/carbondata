<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
-->


## SEGMENT MANAGEMENT

Each load into CarbonData is written into a separate folder called Segment.Segments is a powerful 
concept which helps to maintain consistency of data and easy transaction management.CarbonData provides DML (Data Manipulation Language) commands to maintain the segments.

- [Show Segments](#show-segment)
- [Delete Segment by ID](#delete-segment-by-id)
- [Delete Segment by Date](#delete-segment-by-date)
- [Query Data with Specified Segments](#query-data-with-specified-segments)

### SHOW SEGMENT

  This command is used to list the segments of CarbonData table.

  ```
  SHOW [HISTORY] SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments
  ```

  Example:
  Show visible segments
  ```
  SHOW SEGMENTS FOR TABLE CarbonDatabase.CarbonTable LIMIT 4
  ```
  Show all segments, include invisible segments
  ```
  SHOW HISTORY SEGMENTS FOR TABLE CarbonDatabase.CarbonTable LIMIT 4
  ```

### DELETE SEGMENT BY ID

  This command is used to delete segment by using the segment ID. Each segment has a unique segment ID associated with it. 
  Using this segment ID, you can remove the segment.

  The following command will get the segmentID.

  ```
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments
  ```

  After you retrieve the segment ID of the segment that you want to delete, execute the following command to delete the selected segment.

  ```
  DELETE FROM TABLE [db_name.]table_name WHERE SEGMENT.ID IN (segment_id1, segments_id2, ...)
  ```

  Example:

  ```
  DELETE FROM TABLE CarbonDatabase.CarbonTable WHERE SEGMENT.ID IN (0)
  DELETE FROM TABLE CarbonDatabase.CarbonTable WHERE SEGMENT.ID IN (0,5,8)
  ```

### DELETE SEGMENT BY DATE

  This command will allow to delete the CarbonData segment(s) from the store based on the date provided by the user in the DML command. 
  The segment created before the particular date will be removed from the specific stores.

  ```
  DELETE FROM TABLE [db_name.]table_name WHERE SEGMENT.STARTTIME BEFORE DATE_VALUE
  ```

  Example:
  ```
  DELETE FROM TABLE CarbonDatabase.CarbonTable WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06' 
  ```

### QUERY DATA WITH SPECIFIED SEGMENTS

  This command is used to read data from specified segments during CarbonScan.

  Get the Segment ID:
  ```
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments
  ```

  Set the segment IDs for table
  ```
  SET carbon.input.segments.<database_name>.<table_name> = <list of segment IDs>
  ```

  **NOTE:**
  carbon.input.segments: Specifies the segment IDs to be queried. This property allows you to query specified segments of the specified table. The CarbonScan will read data from specified segments only.

  If user wants to query with segments reading in multi threading mode, then CarbonSession. threadSet can be used instead of SET query.
  ```
  CarbonSession.threadSet ("carbon.input.segments.<database_name>.<table_name>","<list of segment IDs>");
  ```

  Reset the segment IDs
  ```
  SET carbon.input.segments.<database_name>.<table_name> = *;
  ```

  If user wants to query with segments reading in multi threading mode, then CarbonSession. threadSet can be used instead of SET query. 
  ```
  CarbonSession.threadSet ("carbon.input.segments.<database_name>.<table_name>","*");
  ```

  **Examples:**

  * Example to show the list of segment IDs,segment status, and other required details and then specify the list of segments to be read.

  ```
  SHOW SEGMENTS FOR carbontable1;
  
  SET carbon.input.segments.db.carbontable1 = 1,3,9;
  ```

  * Example to query with segments reading in multi threading mode:

  ```
  CarbonSession.threadSet ("carbon.input.segments.db.carbontable_Multi_Thread","1,3");
  ```

  * Example for threadset in multithread environment (following shows how it is used in Scala code):

  ```
  def main(args: Array[String]) {
  Future {          
    CarbonSession.threadSet ("carbon.input.segments.db.carbontable_Multi_Thread","1")
    spark.sql("select count(empno) from carbon.input.segments.db.carbontable_Multi_Thread").show();
     }
   }
  ```

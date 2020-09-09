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


## CLEAN FILES

Clean files command is used to remove the Compacted, Marked For Delete ,In Progress which are stale and partial(Segments which are missing from the table status file but their data is present)
 segments from the store.
 
 Clean Files Command
   ```
   CLEAN FILES FOR TABLE TABLE_NAME
   ```


### TRASH FOLDER

  Carbondata supports a Trash Folder which is used as a redundant folder where all stale carbondata segments are moved to during clean files operation.
  This trash folder is mantained inside the table path. It is a hidden folder(.Trash). The segments that are moved to the trash folder are mantained under a timestamp 
  subfolder(timestamp at which clean files operation is called). This helps the user to list down segments by timestamp.  By default all the timestamp sub-directory have an expiration
  time of (7 days since that timestamp) and it can be configured by the user using the following carbon property
   ```
   carbon.trash.expiration.time = "Number of days"
   ``` 
  Once the timestamp subdirectory is expired as per the configured expiration day value, the subdirectory is deleted from the trash folder in the subsequent clean files command.
  



### DRY RUN
  Support for dry run is provided before the actual clean files operation. This dry run operation will list down all the segments which are going to be manipulated during
  the clean files operation. The dry run result will show the current location of the segment(it can be in FACT folder, Partition folder or trash folder), where that segment
  will be moved(to the trash folder or deleted from store) and the number of days left before it expires once the actual operation will be called. 
  

  ```
  CLEAN FILES FOR TABLE TABLE_NAME options('dry_run'='true')
  ```

### FORCE DELETE TRASH
The force option with clean files command deletes all the files and folders from the trash folder.

  ```
  CLEAN FILES FOR TABLE TABLE_NAME options('force'='true')
  ```

### DATA RECOVERY FROM THE TRASH FOLDER

The segments can be recovered from the trash folder by creating an external table from the desired segment location
in the trash folder and inserting into the original table from the external table. It will create a new segment in the original table.

  ```

  val segment0Path = TrashFolderPath + "/timestamp1/Segment_0/" 

  CREATE EXTERNAL TABLE [IF NOT EXISTS] [db_name.]table_name 
  STORED AS carbondata LOCATION '$segment0Path'
    
  INSERT INTO ORIGINAL_TABLE SELECT * FROM EXTERNAL_TABLE  

  ```




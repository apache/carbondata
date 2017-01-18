<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Troubleshooting
This tutorial is designed to provide troubleshooting for end users and developers 
who are building, deploying, and using CarbonData.

### General Prevention and Best Practices
 * When trying to create a table with a single numeric column, table creation fails: 
   One column that can be considered as dimension is mandatory for table creation.
         
 * "Files locked for updation" when same table is accessed from two or more instances: 
    Remove metastore_db from the examples folder.


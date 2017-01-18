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

# FAQs
* **Auto Compaction not Working**

    The Property carbon.enable.auto.load.merge in carbon.properties need to be set to true.

* **Getting Abstract method error**

    You need to specify the spark version while using Maven to build project.

* **Getting NotImplementedException for subquery using IN and EXISTS**

    Subquery with in and exists not supported in CarbonData.
    
* **Getting Exceptions on creating  a view**
    
    View not supported in CarbonData.
    
* **How to verify if ColumnGroups have been created as desired.**

    Try using desc table query.
    
* **Did anyone try to run CarbonData on windows? Is it supported on Windows?**
    
    We may provide support for windows in future. You are welcome to contribute if you want to add the support :) 
   
    
    


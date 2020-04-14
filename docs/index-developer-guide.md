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

# Index Developer Guide

### Introduction
Index is a data structure that can be used to accelerate certain query of the table. Different Index can be implemented by developers. 
Currently, Carbondata supports three types of Indexes:
1. BloomFilter Index: A space-efficient probabilistic data structure that is used to test whether an element is a member of a set.
2. Lucene Index: High performance, full-featured text search engine.
3. Secondary Index: Sencondary index tables to hold blocklets are created as indexes and managed as child tables internally by Carbondata.

### Index Provider
When user issues `CREATE INDEX index_name ON TABLE main AS 'provider'`, the corresponding IndexProvider implementation will be created and initialized. 
Currently, the provider string can be:
1. class name IndexFactory implementation: Developer can implement new type of Index by extending IndexFactory

When user issues `DROP INDEX index_name ON TABLE main`, the corresponding IndexFactory class will be called.

Click for more details about [Index Management](./index/index-management.md#index-management) and supported [DSL](./index/index-management.md#overview).


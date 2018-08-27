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

#  Data Types

#### CarbonData supports the following data types:

  * Numeric Types
    * SMALLINT
    * INT/INTEGER
    * BIGINT
    * DOUBLE
    * DECIMAL

  * Date/Time Types
    * TIMESTAMP
    * DATE

  * String Types
    * STRING
    * CHAR
    * VARCHAR

    **NOTE**: For string longer than 32000 characters, use `LONG_STRING_COLUMNS` in table property.
    Please refer to TBLProperties in [CreateTable](./ddl-of-carbondata.md#create-table) for more information.

  * Complex Types
    * arrays: ARRAY``<data_type>``
    * structs: STRUCT``<col_name : data_type COMMENT col_comment, ...>``
    
    **NOTE**: Only 2 level complex type schema is supported for now.

  * Other Types
    * BOOLEAN
    
<script>
$(function() {
  // Show selected style on nav item
  $('.b-nav__docs').addClass('selected');

  // Display docs subnav items
  if (!$('.b-nav__docs').parent().hasClass('nav__item__with__subs--expanded')) {
    $('.b-nav__docs').parent().toggleClass('nav__item__with__subs--expanded');
  }
});
</script>

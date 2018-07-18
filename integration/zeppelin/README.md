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

### Please follow below steps to integrate with zeppelin  
1. run ```mvn package -Pzeppelin```
	This will generate _carbondata-zeppelin-*.tar.gz_ under target folder
2. Extract the tar content to _ZEPPELIN_INSTALL_HOME/interpreter/_
3. Add _org.apache.carbonndata.zeppelin.CarbonInterpreter_ to list of interpreters mentioned by _zeppelin.interpreters_ @ _ZEPPELIN_INSTALL_HOME/conf/zeppelin-site.xml_ (create if not exists)
	Example:
```xml
	<property>
	  <name>zeppelin.interpreters</name>
<value>org.apache.zeppelin.spark.SparkInterpreter,.....,org.apache.carbonndata.zeppelin.CarbonInterpreter</value>
	  <description>Comma separated interpreter configurations. First interpreter become a default</description>
	</property>
```
4. Add carbon to list of interpreters mentioned by zeppelin.interpreter.order @ ZEPPELIN_INSTALL_HOME/conf/zeppelin-site.xml
	Example:
```xml
	<property>
	  <name>zeppelin.interpreter.group.order</name>
	  <value>spark,..,carbon</value>
	  <description></description>
	</property>
```

5. Restart Zeppelin server and add new interpreter with name _carbon_ from zeppelin interpreter page
    Refer : https://zeppelin.apache.org/docs/0.8.0/usage/interpreter/overview.html#what-is-zeppelin-interpreter
6. Configure ```carbon.query.api.url``` in interpreter setting from zeppelin interpreter page and click save
7. Now can use notebook with interpreter ```%carbon```

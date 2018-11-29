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

# C++ SDK Guide

CarbonData C++ SDK provides C++ interface to write and read carbon file. 
C++ SDK use JNI to invoke java SDK in C++ code.


# C++ SDK Reader
This C++ SDK reader reads CarbonData file and carbonindex file at a given path.
External client can make use of this reader to read CarbonData files in C++ 
code and without CarbonSession.


In the carbon jars package, there exist a carbondata-sdk.jar, 
including SDK reader for C++ SDK.
## Quick example

Please find example code at  [main.cpp](https://github.com/apache/carbondata/blob/master/store/CSDK/test/main.cpp) of CSDK module  

When users use C++ to read carbon files, users should init JVM first. Then users create 
carbon reader and read data.There are some example code of read data from local disk  
and read data from S3 at main.cpp of CSDK module.  Finally, users need to 
release the memory and destroy JVM.

C++ SDK support read batch row. User can set batch by using withBatch(int batch) before build, and read batch by using readNextBatchRow().

## API List
### CarbonReader
```
    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * @param tableName table name
     * @return CarbonReaderBuilder object
     */
    jobject builder(JNIEnv *env, char *path, char *tableName);
```

```
    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * */
    void builder(JNIEnv *env, char *path);
```

```
    /**
     * Configure the projection column names of carbon reader
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return CarbonReaderBuilder object
     */
    jobject projection(int argc, char *argv[]);
```

```
    /**
     *  build carbon reader with argument vector
     *  it support multiple parameter
     *  like: key=value
     *  for example: fs.s3a.access.key=XXXX, XXXX is user's access key value
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return CarbonReaderBuilder object
     **/
    jobject withHadoopConf(int argc, char *argv[]);
```

```
   /**
     * Sets the batch size of records to read
     *
     * @param batch batch size
     * @return CarbonReaderBuilder object
     */
    void withBatch(int batch);
```

```
    /**
     * Configure Row Record Reader for reading.
     */
    void withRowRecordReader();
```

```
    /**
     * build carbonReader object for reading data
     * it support read data from load disk
     *
     * @return carbonReader object
     */
    jobject build();
```

```
    /**
     * Whether it has next row data
     *
     * @return boolean value, if it has next row, return true. if it hasn't next row, return false.
     */
    jboolean hasNext();
```

```
    /**
     * read next carbonRow from data
     * @return carbonRow object of one row
     */
     jobject readNextRow();
```

```
    /**
     * read Next Batch Row
     *
     * @return rows
     */
    jobjectArray readNextBatchRow();
```

```
    /**
     * close the carbon reader
     *
     * @return  boolean value
     */
    jboolean close();
```

# C++ SDK Writer
This C++ SDK writer writes CarbonData file and carbonindex file at a given path. 
External client can make use of this writer to write CarbonData files in C++ 
code and without CarbonSession. C++ SDK already supports S3 and local disk.

In the carbon jars package, there exist a carbondata-sdk.jar, 
including SDK writer for C++ SDK. 

## Quick example
Please find example code at  [main.cpp](https://github.com/apache/carbondata/blob/master/store/CSDK/test/main.cpp) of CSDK module  

When users use C++ to write carbon files, users should init JVM first. Then users create 
carbon writer and write data.There are some example code of write data to local disk  
and write data to S3 at main.cpp of CSDK module.  Finally, users need to 
release the memory and destroy JVM.

## API List
### CarbonWriter
```
    /**
     * create a CarbonWriterBuilder object for building carbonWriter,
     * CarbonWriterBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @return CarbonWriterBuilder object
     */
    void builder(JNIEnv *env);
```

```
    /**
     * Sets the output path of the writer builder
     *
     * @param path is the absolute path where output files are written
     * This method must be called when building CarbonWriterBuilder
     * @return updated CarbonWriterBuilder
     */
    void outputPath(char *path);
```

```
    /**
     * configure the schema with json style schema
     *
     * @param jsonSchema json style schema
     * @return updated CarbonWriterBuilder
     */
    void withCsvInput(char *jsonSchema);
```

```
    /**
    * Updates the hadoop configuration with the given key value
    *
    * @param key key word
    * @param value value
    * @return CarbonWriterBuilder object
    */
    void withHadoopConf(char *key, char *value);
```

```
    /**
     * @param appName appName which is writing the carbondata files
     */
    void writtenBy(char *appName);
```

```
    /**
     * build carbonWriter object for writing data
     * it support write data from load disk
     *
     * @return carbonWriter object
     */
    void build();
```

```
    /**
     * Write an object to the file, the format of the object depends on the
     * implementation.
     * Note: This API is not thread safe
     */
    void write(jobject obj);
```

```
    /**
     * close the carbon Writer
     */
    void close();
```

### CarbonSchemaReader

```
    /**
     * constructor with jni env
     *
     * @param env  jni env
     */
    CarbonSchemaReader(JNIEnv *env);
```

```
    /**
     * read schema from path,
     * path can be folder path, carbonindex file path, and carbondata file path
     * and will not check all files schema
     *
     * @param path file/folder path
     * @return schema
     */
    jobject readSchema(char *path);
```

```
    /**
     *  read schema from path,
     *  path can be folder path, carbonindex file path, and carbondata file path
     *  and user can decide whether check all files schema
     *
     * @param path carbon data path
     * @param validateSchema whether check all files schema
     * @return schema
     */
    jobject readSchema(char *path, bool validateSchema);
```

### Schema
``` 
 /**
     * constructor with jni env and carbon schema data
     *
     * @param env jni env
     * @param schema  carbon schema data
     */
    Schema(JNIEnv *env, jobject schema);
```

```
    /**
     * get fields length of schema
     *
     * @return fields length
     */
    int getFieldsLength();
```

```
    /**
     * get field name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal field name
     */
    char *getFieldName(int ordinal);
```

```
    /**
     * get  field data type name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal field data type name
     */
    char *getFieldDataTypeName(int ordinal);
```

```
    /**
     * get  array child element data type name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal array child element data type name
     */
    char *getArrayElementTypeName(int ordinal);
```

### CarbonProperties
```
  /**
     * Constructor of CarbonProperties
     *
     * @param env JNI env
     */
    CarbonProperties(JNIEnv *env);
```

```
    /**
     * This method will be used to add a new property
     * 
     * @param key property key
     * @param value property value
     * @return CarbonProperties object
     */
    jobject addProperty(char *key, char *value);
```

```
    /**
     * This method will be used to get the properties value
     *
     * @param key  property key
     * @return  property value
     */
    char *getProperty(char *key);
```

```
    /**
     * This method will be used to get the properties value
     * if property is not present then it will return the default value
     *
     * @param key  property key
     * @param defaultValue  property default Value
     * @return
     */
    char *getProperty(char *key, char *defaultValue);
```

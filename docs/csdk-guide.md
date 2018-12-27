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
 * Create a CarbonReaderBuilder object for building carbonReader,
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
 * Create a CarbonReaderBuilder object for building carbonReader,
 * CarbonReaderBuilder object can configure different parameter
 *
 * @param env JNIEnv
 * @param path data store path
 * 
 */
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
 * Build carbon reader with argument vector
 * it supports multiple parameters
 * like: key=value
 * for example: fs.s3a.access.key=XXXX, XXXX is user's access key value
 *
 * @param argc argument counter
 * @param argv argument vector
 * @return CarbonReaderBuilder object
 *
 */
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
 * Build carbonReader object for reading data
 * it supports read data from load disk
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
 * Read next carbonRow from data
 * @return carbonRow object of one row
 */
jobject readNextRow();
```

```
/**
 * Read Next Batch Row
 *
 * @return rows
 */
jobjectArray readNextBatchRow();
```

```
/**
 * Close the carbon reader
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
 * Create a CarbonWriterBuilder object for building carbonWriter,
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
 * Sets the list of columns that needs to be in sorted order
 *
 * @param argc argc argument counter, the number of projection column
 * @param argv argv is a string array of columns that needs to be sorted.
 *                  If it is null or by default all dimensions are selected for sorting
 *                  If it is empty array, no columns are sorted
 */
void sortBy(int argc, char *argv[]);
```

```
/**
 * Configure the schema with json style schema
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
 * To support the table properties for writer
 *
 * @param key properties key
 * @param value properties value
 */
void withTableProperty(char *key, char *value);
```

```
/**
 * To support the load options for C++ sdk writer
 *
 * @param options key,value pair of load options.
 * supported keys values are
 * a. bad_records_logger_enable -- true (write into separate logs), false
 * b. bad_records_action -- FAIL, FORCE, IGNORE, REDIRECT
 * c. bad_record_path -- path
 * d. dateformat -- same as JAVA SimpleDateFormat
 * e. timestampformat -- same as JAVA SimpleDateFormat
 * f. complex_delimiter_level_1 -- value to Split the complexTypeData
 * g. complex_delimiter_level_2 -- value to Split the nested complexTypeData
 * h. quotechar
 * i. escapechar
 *
 * Default values are as follows.
 *
 * a. bad_records_logger_enable -- "false"
 * b. bad_records_action -- "FAIL"
 * c. bad_record_path -- ""
 * d. dateformat -- "" , uses from carbon.properties file
 * e. timestampformat -- "", uses from carbon.properties file
 * f. complex_delimiter_level_1 -- "$"
 * g. complex_delimiter_level_2 -- ":"
 * h. quotechar -- "\""
 * i. escapechar -- "\\"
 *
 * @return updated CarbonWriterBuilder
 */
void withLoadOption(char *key, char *value);
```

```
/**
 * Sets the taskNo for the writer. CSDKs concurrently running
 * will set taskNo in order to avoid conflicts in file's name during write.
 *
 * @param taskNo is the TaskNo user wants to specify.
 *               by default it is system time in nano seconds.
 */
void taskNo(long taskNo);
```

```
/**
 * Set the timestamp in the carbondata and carbonindex index files
 *
 * @param timestamp is a timestamp to be used in the carbondata and carbonindex index files.
 * By default set to zero.
 * @return updated CarbonWriterBuilder
 */
void uniqueIdentifier(long timestamp);
```

```
/**
 * To make c++ sdk writer thread safe.
 *
 * @param numOfThreads should number of threads in which writer is called in multi-thread scenario
 *                      default C++ sdk writer is not thread safe.
 *                      can use one writer instance in one thread only.
 */
void withThreadSafe(short numOfThreads) ;
```

```
/**
 * To set the carbondata file size in MB between 1MB-2048MB
 *
 * @param blockSize is size in MB between 1MB to 2048 MB
 * default value is 1024 MB
 */
void withBlockSize(int blockSize);
```

```
/**
 * To set the blocklet size of CarbonData file
 *
 * @param blockletSize is blocklet size in MB
 *        default value is 64 MB
 * @return updated CarbonWriterBuilder
 */
void withBlockletSize(int blockletSize);
```

```
/**
 * @param localDictionaryThreshold is localDictionaryThreshold, default is 10000
 * @return updated CarbonWriterBuilder
 */
void localDictionaryThreshold(int localDictionaryThreshold);
```

```
/**
 * @param enableLocalDictionary enable local dictionary, default is false
 * @return updated CarbonWriterBuilder
 */
void enableLocalDictionary(bool enableLocalDictionary);
```

```
/**
 * @param appName appName which is writing the carbondata files
 */
void writtenBy(char *appName);
```

```
/**
 * Build carbonWriter object for writing data
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
 * Constructor with jni env
 *
 * @param env  jni env
 */
CarbonSchemaReader(JNIEnv *env);
```

```
/**
 * Read schema from path,
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
 * Read schema from path,
 * path can be folder path, carbonindex file path, and carbondata file path
 * and user can decide whether check all files schema
 *
 * @param path carbon data path
 * @param validateSchema whether check all files schema
 * @return schema
 */
jobject readSchema(char *path, bool validateSchema);
```

```
/**
 * Read schema from path,
 * path can be folder path, carbonindex file path, and carbondata file path
 * and will not check all files schema
 *
 * @param path file/folder path
 * @param conf           configuration support, can set s3a AK,SK,
 *                       end point and other conf with this
 * @return schema
 */
jobject readSchema(char *path, Configuration conf);
```

```
/**
 * Read schema from path,
 * path can be folder path, carbonindex file path, and carbondata file path
 * and user can decide whether check all files schema
 *
 * @param path carbon data path
 * @param validateSchema whether check all files schema
 * @param conf           configuration support, can set s3a AK,SK,
 *                       end point and other conf with this
 * @return schema
 */
jobject readSchema(char *path, bool validateSchema, Configuration conf);
```

### Schema
```
/**
 * Constructor with jni env and carbon schema data
 *
 * @param env jni env
 * @param schema  carbon schema data
 */
Schema(JNIEnv *env, jobject schema);
```

```
/**
 * Get fields length of schema
 *
 * @return fields length
 */
int getFieldsLength();
```

```
/**
 * Get field name by ordinal
 *
 * @param ordinal the data index of carbon schema
 * @return ordinal field name
 */
char *getFieldName(int ordinal);
```

```
/**
 * Get  field data type name by ordinal
 *
 * @param ordinal the data index of carbon schema
 * @return ordinal field data type name
 */
char *getFieldDataTypeName(int ordinal);
```

```
/**
 * Get  array child element data type name by ordinal
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
 * @param key property key
 * @return property value
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

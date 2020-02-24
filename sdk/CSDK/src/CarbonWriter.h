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

#include <jni.h>

class CarbonWriter {
private:
    /**
    * jni env
    */
    JNIEnv *jniEnv = NULL;

    /**
     * carbonWriterBuilder object for building carbonWriter
     * it can configure some operation
     */
    jobject carbonWriterBuilderObject = NULL;

    /**
     * carbonWriter object for writing data
     */
    jobject carbonWriterObject = NULL;

    /**
     * carbon writer class
     */
    jclass carbonWriter = NULL;

    /**
     * write method id
     */
    jmethodID writeID = NULL;

    /**
     * check whether has called builder
     *
     * @return true or throw exception
     */
    bool checkBuilder();

    /**
     * check writer whether has called build
     *
     * @return true or throw exception
     */
    bool checkWriter();
public:
    /**
     * create a CarbonWriterBuilder object for building carbonWriter,
     * CarbonWriterBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @return CarbonWriterBuilder object
     */
    void builder(JNIEnv *env);

    /**
     * Sets the output path of the writer builder
     *
     * @param path is the absolute path where output files are written
     * This method must be called when building CarbonWriterBuilder
     * @return updated CarbonWriterBuilder
     */
    void outputPath(char *path);

     /**
      * sets the list of columns that needs to be in sorted order
      *
      * @param argc argc argument counter, the number of projection column
      * @param argv argv is a string array of columns that needs to be sorted.
      *                  If it is null or by default all dimensions are selected for sorting
      *                  If it is empty array, no columns are sorted
      */
    void sortBy(int argc, char *argv[]);

    /**
     * configure the schema with json style schema
     *
     * @param jsonSchema json style schema
     * @return updated CarbonWriterBuilder
     */
    void withCsvInput(char *jsonSchema);

    /**
     * configure the schema
     *
     * @return updated CarbonWriterBuilder
     */
    void withCsvInput( );

    /**
    * configure parameter, including ak,sk and endpoint
    *
    * @param key key word
    * @param value value
    * @return CarbonWriterBuilder object
    */
    void withHadoopConf(char *key, char *value);

    /**
     *  To support the table properties for writer
     *
     * @param key properties key
     * @param value properties value
     */
    void withTableProperty(char *key, char *value);

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

    /**
     * sets the taskNo for the writer. CSDKs concurrently running
     * will set taskNo in order to avoid conflicts in file's name during write.
     *
     * @param taskNo is the TaskNo user wants to specify.
     *               by default it is system time in nano seconds.
     */
    void taskNo(long taskNo);

    /**
     * to set the timestamp in the carbondata and carbonindex index files
     *
     * @param timestamp is a timestamp to be used in the carbondata and carbonindex index files.
     * By default set to zero.
     * @return updated CarbonWriterBuilder
     */
    void uniqueIdentifier(long timestamp);

    /**
     * To make c++ sdk writer thread safe.
     *
     * @param numOfThreads should number of threads in which writer is called in multi-thread scenario
     *                      default C++ sdk writer is not thread safe.
     *                      can use one writer instance in one thread only.
     */
    void withThreadSafe(short numOfThreads) ;

    /**
     * To set the carbondata file size in MB between 1MB-2048MB
     *
     * @param blockSize is size in MB between 1MB to 2048 MB
     * default value is 1024 MB
     */
    void withBlockSize(int blockSize);

    /**
     * To set the blocklet size of CarbonData file
     *
     * @param blockletSize is blocklet size in MB
     *        default value is 64 MB
     * @return updated CarbonWriterBuilder
     */
    void withBlockletSize(int blockletSize);

    /**
     * To set the path of carbon schema file
     * @param schemaFilePath The path of carbon schema file
     */
    void withSchemaFile(char *schemaFilePath);

    /**
     * @param localDictionaryThreshold is localDictionaryThreshold, default is 10000
     * @return updated CarbonWriterBuilder
     */
    void localDictionaryThreshold(int localDictionaryThreshold);

    /**
     * @param enableLocalDictionary enable local dictionary, default is false
     * @return updated CarbonWriterBuilder
     */
    void enableLocalDictionary(bool enableLocalDictionary);

    /**
     * @param appName appName which is writing the carbondata files
     */
    void writtenBy(char *appName);

    /**
     * build carbonWriter object for writing data
     * it support write data from load disk
     *
     * @return carbonWriter object
     */
    void build();

    /**
     * Write an object to the file, the format of the object depends on the
     * implementation.
     * Note: This API is not thread safe
     */
    void write(jobject obj);

    /**
     * close the carbon Writer
     */
    void close();
};



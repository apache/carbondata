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

class CarbonReader {
private:
    /**
     * hasNext jmethodID
     */
    jmethodID hasNextID = NULL;

    /**
     * readNextRow jmethodID
     */
    jmethodID readNextRowID = NULL;

    /**
     * readNextBatchRow jmethodID
     */
    jmethodID readNextBatchRowID = NULL;

    /**
     * carbonReaderBuilder object for building carbonReader
     * it can configure some operation
     */
    jobject carbonReaderBuilderObject = NULL;

    /**
     * carbonReader object for reading data
     */
    jobject carbonReaderObject = NULL;

    /**
    * carbonReader class for reading data
    */
    jclass carbonReaderClass = NULL;

    /**
     * Return true if carbonReaderBuilder Object isn't NULL
     * Throw exception if carbonReaderBuilder Object is NULL
     *
     * @return true or throw exception
     */
    bool checkBuilder();

    /**
     * Return true if carbonReader Object isn't NULL
     * Throw exception if carbonReader Object is NULL
     *
     * @return true or throw exception
     */
    bool checkReader();
public:

    /**
     * jni env
     */
    JNIEnv *jniEnv = NULL;

    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * @param tableName table name
     */
    void builder(JNIEnv *env, char *path, char *tableName);

    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * */
    void builder(JNIEnv *env, char *path);

    /**
     * Configure the projection column names of carbon reader
     *
     * @param argc argument counter, the number of projection column
     * @param argv argument vector, projection column names
     */
    void projection(int argc, char *argv[]);

    /**
     * configure parameter, including ak,sk and endpoint
     *
     * @param key key word
     * @param value value
     */
    void withHadoopConf(char *key, char *value);

    /**
     * Sets the batch size of records to read
     *
     * @param batch batch size
     * @return CarbonReaderBuilder object
     */
    void withBatch(int batch);

    /**
     * Configure Row Record Reader for reading.
     */
    void withRowRecordReader();

    /**
     * build carbonReader object for reading data
     * it support read data from load disk
     *
     * @return carbonReader object
     */
    jobject build();

    /**
     * Whether it has next row data
     *
     * @return boolean value, if it has next row, return true. if it hasn't next row, return false.
     */
    jboolean hasNext();

    /**
     * read next carbon Row from data
     * @return carbon Row object of one row
     */
    jobject readNextRow();

    /**
     * read Next Batch Row
     *
     * @return rows
     */
    jobjectArray readNextBatchRow();

    /**
     * close the carbon reader
     *
     * @return  boolean value
     */
    void close();
};

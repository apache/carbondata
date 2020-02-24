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

class Schema {
private:

    /**
     * jni env
     */
    JNIEnv *jniEnv = NULL;

    /**
     * schema class for get method id
     */
    jclass schemaClass = NULL;

    /**
     * carbon schema data
     */
    jobject schema = NULL;

    /**
     * check ordinal, ordinal can't be negative
     *
     * @param ordinal int value, the data index of carbon Row
     */
    void checkOrdinal(int ordinal);

public:

    /**
     * constructor with jni env and carbon schema data
     *
     * @param env jni env
     * @param schema  carbon schema data
     */
    Schema(JNIEnv *env, jobject schema);

    /**
     * get fields length of schema
     *
     * @return fields length
     */
    int getFieldsLength();

    /**
     * get field name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal field name
     */
    char *getFieldName(int ordinal);

    /**
     * get  field data type name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal field data type name
     */
    char *getFieldDataTypeName(int ordinal);

    /**
     * get  array child element data type name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal array child element data type name
     */
    char *getArrayElementTypeName(int ordinal);
};
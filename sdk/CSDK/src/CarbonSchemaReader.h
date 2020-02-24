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
#include "Configuration.h"

class CarbonSchemaReader {
private:

    /**
     * jni env
     */
    JNIEnv *jniEnv = NULL;

    /**
     * carbonSchemaReader Class for get method id and call method
     */
    jclass carbonSchemaReaderClass = NULL;

public:

    /**
     * constructor with jni env
     *
     * @param env  jni env
     */
    CarbonSchemaReader(JNIEnv *env);

    /**
     * read schema from path,
     * path can be folder path, carbonindex file path, and carbondata file path
     * and will not check all files schema
     *
     * @param path file/folder path
     * @return schema
     */
    jobject readSchema(char *path);

    /**
     * read schema from path,
     * path can be folder path, carbonindex file path, and carbondata file path
     * and will not check all files schema
     *
     * @param path file/folder path
     * @param conf           configuration support, can set s3a AK,SK,
     *                       end point and other conf with this
     * @return schema
     */
    jobject readSchema(char *path, Configuration conf);

    /**
     *  read schema from path,
     *  path can be folder path, carbonindex file path, and carbondata file path
     *  and user can decide whether check all files schema
     *
     * @param path carbon data path
     * @param validateSchema whether check all files schema
     * @param conf           configuration support, can set s3a AK,SK,
     *                       end point and other conf with this
     * @return schema
     */
    jobject readSchema(char *path, bool validateSchema, Configuration conf);

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

};
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

class CarbonSchemaReader {
private:

    /**
     * jni env
     */
    JNIEnv *jniEnv;

    /**
     * carbonSchemaReader Class for get method id and call method
     */
    jclass carbonSchemaReaderClass;

public:

    /**
     * constructor with jni env
     *
     * @param env  jni env
     */
    CarbonSchemaReader(JNIEnv *env);

    /**
     * read Schema from Data File
     *
     * @param path Data File path
     * @return carbon schema object
     */
    jobject readSchemaInDataFile(char *path);

    /**
     * read Schema from index File
     *
     * @param path index File path
     * @return carbon schema object
     */
    jobject readSchemaInIndexFile(char *path);

};
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

#include <stdexcept>
#include "CarbonSchemaReader.h"

CarbonSchemaReader::CarbonSchemaReader(JNIEnv *env) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    this->carbonSchemaReaderClass = env->FindClass("org/apache/carbondata/sdk/file/CarbonSchemaReader");
    if (carbonSchemaReaderClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/CarbonSchemaReader");
    }
    this->jniEnv = env;
}

jobject CarbonSchemaReader::readSchema(char *path) {
    Configuration conf(jniEnv);
    return readSchema(path, conf);
}

jobject CarbonSchemaReader::readSchema(char *path, Configuration conf) {
    if (path == NULL) {
        throw std::runtime_error("path parameter can't be NULL.");
    }
    jmethodID methodID = jniEnv->GetStaticMethodID(carbonSchemaReaderClass, "readSchema",
        "(Ljava/lang/String;Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/carbondata/sdk/file/Schema;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: readSchema");
    }
    jstring jPath = jniEnv->NewStringUTF(path);
    jvalue args[2];
    args[0].l = jPath;
    args[1].l = conf.getConfigurationObject();
    jobject result = jniEnv->CallStaticObjectMethodA(carbonSchemaReaderClass, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

jobject CarbonSchemaReader::readSchema(char *path, bool validateSchema, Configuration conf) {
    if (path == NULL) {
        throw std::runtime_error("path parameter can't be NULL.");
    }
    jmethodID methodID = jniEnv->GetStaticMethodID(carbonSchemaReaderClass, "readSchema",
        "(Ljava/lang/String;ZLorg/apache/hadoop/conf/Configuration;)Lorg/apache/carbondata/sdk/file/Schema;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: readSchema");
    }
    jstring jPath = jniEnv->NewStringUTF(path);
    jvalue args[3];
    args[0].l = jPath;
    args[1].z = validateSchema;
    args[2].l = conf.getConfigurationObject();
    jobject result = jniEnv->CallStaticObjectMethodA(carbonSchemaReaderClass, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

jobject CarbonSchemaReader::readSchema(char *path, bool validateSchema) {
    Configuration conf(jniEnv);
    return readSchema(path, validateSchema, conf);
}
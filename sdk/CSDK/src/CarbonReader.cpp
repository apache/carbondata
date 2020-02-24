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
#include <stdexcept>
#include <sys/time.h>
#include "CarbonReader.h"

void CarbonReader::builder(JNIEnv *env, char *path, char *tableName) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    if (path == NULL) {
        throw std::runtime_error("path parameter can't be NULL.");
    }
    if (tableName == NULL) {
        throw std::runtime_error("tableName parameter can't be NULL.");
    }
    jniEnv = env;
    carbonReaderClass = env->FindClass("org/apache/carbondata/sdk/file/CarbonReader");
    if (carbonReaderClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/CarbonReader");
    }
    jmethodID carbonReaderBuilderID = env->GetStaticMethodID(carbonReaderClass, "builder",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    if (carbonReaderBuilderID == NULL) {
        throw std::runtime_error("Can't find the method in java: builder");
    }
    jstring jPath = env->NewStringUTF(path);
    jstring jTableName = env->NewStringUTF(tableName);
    jvalue args[2];
    args[0].l = jPath;
    args[1].l = jTableName;
    carbonReaderBuilderObject = env->CallStaticObjectMethodA(carbonReaderClass, carbonReaderBuilderID, args);
}

void CarbonReader::builder(JNIEnv *env, char *path) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    if (path == NULL) {
        throw std::runtime_error("path parameter can't be NULL.");
    }
    jniEnv = env;
    carbonReaderClass = env->FindClass("org/apache/carbondata/sdk/file/CarbonReader");
    if (carbonReaderClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/CarbonReader");
    }
    jmethodID carbonReaderBuilderID = env->GetStaticMethodID(carbonReaderClass, "builder",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    if (carbonReaderBuilderID == NULL) {
        throw std::runtime_error("Can't find the method in java: builder");
    }
    jstring jPath = env->NewStringUTF(path);
    jvalue args[1];
    args[0].l = jPath;
    carbonReaderBuilderObject = env->CallStaticObjectMethodA(carbonReaderClass, carbonReaderBuilderID, args);
}

bool CarbonReader::checkBuilder() {
    if (carbonReaderBuilderObject == NULL) {
        throw std::runtime_error("carbonReaderBuilder Object can't be NULL. Please call builder method first.");
    }
}

void CarbonReader::projection(int argc, char *argv[]) {
    if (argc < 0) {
        throw std::runtime_error("argc parameter can't be negative.");
    }
    if (argv == NULL) {
        throw std::runtime_error("argv parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonReaderBuilderClass, "projection",
        "([Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: projection");
    }
    jclass objectArrayClass = jniEnv->FindClass("Ljava/lang/String;");
    if (objectArrayClass == NULL) {
        throw std::runtime_error("Can't find the class in java: java/lang/String");
    }
    jobjectArray array = jniEnv->NewObjectArray(argc, objectArrayClass, NULL);
    for (int i = 0; i < argc; ++i) {
        jstring value = jniEnv->NewStringUTF(argv[i]);
        jniEnv->SetObjectArrayElement(array, i, value);
    }

    jvalue args[1];
    args[0].l = array;
    carbonReaderBuilderObject = jniEnv->CallObjectMethodA(carbonReaderBuilderObject, methodID, args);
}

void CarbonReader::withHadoopConf(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID id = jniEnv->GetMethodID(carbonReaderBuilderClass, "withHadoopConf",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    if (id == NULL) {
        throw std::runtime_error("Can't find the method in java: withHadoopConf");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    carbonReaderBuilderObject = jniEnv->CallObjectMethodA(carbonReaderBuilderObject, id, args);
}

void CarbonReader::withBatch(int batch) {
    checkBuilder();
    if (batch < 1) {
        throw std::runtime_error("batch parameter can't be negative and 0.");
    }
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "withBatch",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    if (buildID == NULL) {
        throw std::runtime_error("Can't find the method in java: withBatch.");
    }
    jvalue args[1];
    args[0].i = batch;
    carbonReaderBuilderObject = jniEnv->CallObjectMethodA(carbonReaderBuilderObject, buildID, args);
}

void CarbonReader::withRowRecordReader() {
    checkBuilder();
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "withRowRecordReader",
        "()Lorg/apache/carbondata/sdk/file/CarbonReaderBuilder;");
    if (buildID == NULL) {
        throw std::runtime_error("Can't find the method in java: withRowRecordReader.");
    }
    carbonReaderBuilderObject = jniEnv->CallObjectMethod(carbonReaderBuilderObject, buildID);
}

jobject CarbonReader::build() {
    checkBuilder();
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonReaderBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonReaderBuilderClass, "build",
        "()Lorg/apache/carbondata/sdk/file/CarbonReader;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: build");
    }
    carbonReaderObject = jniEnv->CallObjectMethod(carbonReaderBuilderObject, methodID);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return carbonReaderObject;
}

bool CarbonReader::checkReader() {
    if (carbonReaderObject == NULL) {
        throw std::runtime_error("carbonReader Object is NULL, Please call build first.");
    }
}

jboolean CarbonReader::hasNext() {
    checkReader();
    if (hasNextID == NULL) {
        jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
        hasNextID = jniEnv->GetMethodID(carbonReader, "hasNext", "()Z");
        if (hasNextID == NULL) {
            throw std::runtime_error("Can't find the method in java: hasNext");
        }
    }
    jboolean result = jniEnv->CallBooleanMethod(carbonReaderObject, hasNextID);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

jobject CarbonReader::readNextRow() {
    checkReader();
    if (readNextRowID == NULL) {
        jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
        readNextRowID = jniEnv->GetMethodID(carbonReader, "readNextRow",
            "()Ljava/lang/Object;");
        if (readNextRowID == NULL) {
            throw std::runtime_error("Can't find the method in java: readNextRow");
        }
    }
    jobject result = jniEnv->CallObjectMethod(carbonReaderObject, readNextRowID);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

jobjectArray CarbonReader::readNextBatchRow() {
    if (readNextBatchRowID == NULL) {
        jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
        readNextBatchRowID = jniEnv->GetMethodID(carbonReader, "readNextBatchRow",
            "()[Ljava/lang/Object;");
        if (readNextBatchRowID == NULL) {
            throw std::runtime_error("Can't find the method in java: readNextBatchRow");
        }
    }
    return (jobjectArray) jniEnv->CallObjectMethod(carbonReaderObject, readNextBatchRowID);
}

void CarbonReader::close() {
    checkReader();
    jclass carbonReader = jniEnv->GetObjectClass(carbonReaderObject);
    jmethodID closeID = jniEnv->GetMethodID(carbonReader, "close", "()V");
    if (closeID == NULL) {
        throw std::runtime_error("Can't find the method in java: close");
    }
    jniEnv->CallBooleanMethod(carbonReaderObject, closeID);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    jniEnv->DeleteLocalRef(carbonReaderBuilderObject);
    jniEnv->DeleteLocalRef(carbonReaderObject);
    jniEnv->DeleteLocalRef(carbonReaderClass);
}
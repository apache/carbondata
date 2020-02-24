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
#include "Schema.h"

Schema::Schema(JNIEnv *env, jobject schema) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    if (schema == NULL) {
        throw std::runtime_error("schema parameter can't be NULL.");
    }
    this->schemaClass = env->FindClass("org/apache/carbondata/sdk/file/Schema");
    if (schemaClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/Schema");
    }
    this->jniEnv = env;
    this->schema = schema;
}

int Schema::getFieldsLength() {
    jmethodID methodID = jniEnv->GetMethodID(schemaClass, "getFieldsLength",
        "()I");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: getFieldsLength");
    }
    return jniEnv->CallIntMethod(schema, methodID);
};

void Schema::checkOrdinal(int ordinal) {
    if (ordinal < 0) {
        throw std::runtime_error("ordinal parameter can't be negative.");
    }
}

char *Schema::getFieldName(int ordinal) {
    checkOrdinal(ordinal);
    jmethodID methodID = jniEnv->GetMethodID(schemaClass, "getFieldName",
        "(I)Ljava/lang/String;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: getFieldName");
    }
    jvalue args[1];
    args[0].i = ordinal;
    jobject fieldName = jniEnv->CallObjectMethodA(schema, methodID, args);
    return (char *) jniEnv->GetStringUTFChars((jstring) fieldName, JNI_FALSE);
};

char *Schema::getFieldDataTypeName(int ordinal) {
    checkOrdinal(ordinal);
    jmethodID methodID = jniEnv->GetMethodID(schemaClass, "getFieldDataTypeName",
        "(I)Ljava/lang/String;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: getFieldDataTypeName");
    }
    jvalue args[1];
    args[0].i = ordinal;
    jobject fieldName = jniEnv->CallObjectMethodA(schema, methodID, args);
    return (char *) jniEnv->GetStringUTFChars((jstring) fieldName, JNI_FALSE);
};

char *Schema::getArrayElementTypeName(int ordinal) {
    checkOrdinal(ordinal);
    jmethodID methodID = jniEnv->GetMethodID(schemaClass, "getArrayElementTypeName",
        "(I)Ljava/lang/String;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: getArrayElementTypeName");
    }
    jvalue args[1];
    args[0].i = ordinal;
    jobject fieldName = jniEnv->CallObjectMethodA(schema, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return (char *) jniEnv->GetStringUTFChars((jstring) fieldName, JNI_FALSE);
};

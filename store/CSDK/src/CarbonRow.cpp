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
#include <cstring>
#include <stdexcept>
#include "CarbonRow.h"

CarbonRow::CarbonRow(JNIEnv *env) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    this->rowUtilClass = env->FindClass("org/apache/carbondata/sdk/file/RowUtil");
    if (rowUtilClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/RowUtil");
    }
    this->jniEnv = env;
    getShortId = jniEnv->GetStaticMethodID(rowUtilClass, "getShort",
        "([Ljava/lang/Object;I)S");
    if (getShortId == NULL) {
        throw std::runtime_error("Can't find the method in java: getShort");
    }

    getIntId = jniEnv->GetStaticMethodID(rowUtilClass, "getInt",
        "([Ljava/lang/Object;I)I");
    if (getIntId == NULL) {
        throw std::runtime_error("Can't find the method in java: getInt");
    }
    getLongId = jniEnv->GetStaticMethodID(rowUtilClass, "getLong",
        "([Ljava/lang/Object;I)J");
    if (getLongId == NULL) {
        throw std::runtime_error("Can't find the method in java: getLong");
    }

    getDoubleId = jniEnv->GetStaticMethodID(rowUtilClass, "getDouble",
        "([Ljava/lang/Object;I)D");
    if (getDoubleId == NULL) {
        throw std::runtime_error("Can't find the method in java: getDouble");
    }

    getFloatId = jniEnv->GetStaticMethodID(rowUtilClass, "getFloat",
        "([Ljava/lang/Object;I)F");
    if (getFloatId == NULL) {
        throw std::runtime_error("Can't find the method in java: getFloat");
    }

    getBooleanId = jniEnv->GetStaticMethodID(rowUtilClass, "getBoolean",
        "([Ljava/lang/Object;I)Z");
    if (getBooleanId == NULL) {
        throw std::runtime_error("Can't find the method in java: getBoolean");
    }

    getStringId = jniEnv->GetStaticMethodID(rowUtilClass, "getString",
        "([Ljava/lang/Object;I)Ljava/lang/String;");
    if (getStringId == NULL) {
        throw std::runtime_error("Can't find the method in java: getString");
    }

    getDecimalId = jniEnv->GetStaticMethodID(rowUtilClass, "getDecimal",
        "([Ljava/lang/Object;I)Ljava/lang/String;");
    if (getDecimalId == NULL) {
        throw std::runtime_error("Can't find the method in java: getDecimal");
    }

    getVarcharId = jniEnv->GetStaticMethodID(rowUtilClass, "getVarchar",
        "([Ljava/lang/Object;I)Ljava/lang/String;");
    if (getVarcharId == NULL) {
        throw std::runtime_error("Can't find the method in java: getVarchar");
    }

    getArrayId = jniEnv->GetStaticMethodID(rowUtilClass, "getArray",
        "([Ljava/lang/Object;I)[Ljava/lang/Object;");
    if (getArrayId == NULL) {
        throw std::runtime_error("Can't find the method in java: getArray");
    }
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonRow::setCarbonRow(jobject data) {
    if (data == NULL) {
        throw std::runtime_error("data parameter can't be NULL.");
    }
    this->carbonRow = data;
}

void CarbonRow::checkOrdinal(int ordinal) {
    if (ordinal < 0) {
        throw std::runtime_error("ordinal parameter can't be negative.");
    }
}

void CarbonRow::checkCarbonRow() {
    if (carbonRow == NULL) {
        throw std::runtime_error("carbonRow is NULL! Please set carbonRow first..");
    }
}

short CarbonRow::getShort(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    short result = jniEnv->CallStaticShortMethodA(rowUtilClass, getShortId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

int CarbonRow::getInt(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    int result = jniEnv->CallStaticIntMethodA(rowUtilClass, getIntId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

long CarbonRow::getLong(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    long result = jniEnv->CallStaticLongMethodA(rowUtilClass, getLongId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

double CarbonRow::getDouble(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    double result = jniEnv->CallStaticDoubleMethodA(rowUtilClass, getDoubleId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

float CarbonRow::getFloat(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    float result = jniEnv->CallStaticFloatMethodA(rowUtilClass, getFloatId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

jboolean CarbonRow::getBoolean(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    bool result = jniEnv->CallStaticBooleanMethodA(rowUtilClass, getBooleanId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

char *CarbonRow::getString(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobject data = jniEnv->CallStaticObjectMethodA(rowUtilClass, getStringId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    jniEnv->DeleteLocalRef(data);
    return str;
}

char *CarbonRow::getDecimal(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobject data = jniEnv->CallStaticObjectMethodA(rowUtilClass, getDecimalId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    jniEnv->DeleteLocalRef(data);
    return str;
}

char *CarbonRow::getVarchar(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobject data = jniEnv->CallStaticObjectMethodA(rowUtilClass, getVarcharId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    jniEnv->DeleteLocalRef(data);
    return str;
}

jobjectArray CarbonRow::getArray(int ordinal) {
    checkCarbonRow();
    checkOrdinal(ordinal);
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobjectArray result = (jobjectArray) jniEnv->CallStaticObjectMethodA(rowUtilClass, getArrayId, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    return result;
}

void CarbonRow::close() {
    jniEnv->DeleteLocalRef(rowUtilClass);
    jniEnv->DeleteLocalRef(carbonRow);
}
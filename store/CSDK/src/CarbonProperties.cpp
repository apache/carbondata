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
#include "CarbonProperties.h"

CarbonProperties::CarbonProperties(JNIEnv *env) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    this->jniEnv = env;
    this->carbonPropertiesClass = jniEnv->FindClass("org/apache/carbondata/core/util/CarbonProperties");
    if (carbonPropertiesClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/core/util/CarbonProperties");
    }
    jmethodID id = jniEnv->GetStaticMethodID(carbonPropertiesClass, "getInstance",
        "()Lorg/apache/carbondata/core/util/CarbonProperties;");
    if (id == NULL) {
        throw std::runtime_error("Can't find the method in java: getInstance");
    }
    this->carbonPropertiesObject = jniEnv->CallStaticObjectMethod(carbonPropertiesClass, id);
}


jobject CarbonProperties::addProperty(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    jmethodID id = jniEnv->GetMethodID(carbonPropertiesClass, "addProperty",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/core/util/CarbonProperties;");
    if (id == NULL) {
        throw std::runtime_error("Can't find the method in java: addProperty");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);;
    args[1].l = jniEnv->NewStringUTF(value);;
    this->carbonPropertiesObject = jniEnv->CallObjectMethodA(carbonPropertiesObject, id, args);
    return carbonPropertiesObject;
}

char *CarbonProperties::getProperty(char *key) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    jmethodID id = jniEnv->GetMethodID(carbonPropertiesClass, "getProperty",
        "(Ljava/lang/String;)Ljava/lang/String;");
    if (id == NULL) {
        throw std::runtime_error("Can't find the method in java: getProperty");
    }
    jvalue args[1];
    args[0].l = jniEnv->NewStringUTF(key);
    jobject value = jniEnv->CallObjectMethodA(carbonPropertiesObject, id, args);
    if (value == NULL) {
        return NULL;
    }
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) value, JNI_FALSE);
    jniEnv->DeleteLocalRef(value);
    return str;
}

char *CarbonProperties::getProperty(char *key, char *defaultValue) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (defaultValue == NULL) {
        throw std::runtime_error("defaultValue parameter can't be NULL.");
    }
    jmethodID id = jniEnv->GetMethodID(carbonPropertiesClass, "getProperty",
        "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
    if (id == NULL) {
        throw std::runtime_error("Can't find the method in java: getProperty");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(defaultValue);
    jobject value = jniEnv->CallObjectMethodA(carbonPropertiesObject, id, args);
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) value, JNI_FALSE);
    jniEnv->DeleteLocalRef(value);
    return str;
}

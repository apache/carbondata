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
#include "Configuration.h"

Configuration::Configuration(JNIEnv *env) {
    this->jniEnv = env;
    configurationClass = env->FindClass("org/apache/hadoop/conf/Configuration");
    if (configurationClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/hadoop/conf/Configuration");
    }

    initID = jniEnv->GetMethodID(configurationClass, "<init>", "()V");
    if (initID == NULL) {
        throw std::runtime_error("Can't find init it in java: org/apache/hadoop/conf/Configuration");
    }
    configurationObject = jniEnv->NewObject(configurationClass, initID);
    if (configurationClass == NULL) {
        throw std::runtime_error("Can't create object in java: org/apache/hadoop/conf/Configuration");
    }
}

void Configuration::check() {
    if (configurationClass == NULL) {
        throw std::runtime_error("configurationClass can't be NULL. Please init first.");
    }
    if (configurationObject == NULL) {
        throw std::runtime_error("configurationObject can't be NULL. Please init first.");
    }
}

void Configuration::set(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    check();

    if (setID == NULL) {
        setID = jniEnv->GetMethodID(configurationClass, "set",
              "(Ljava/lang/String;Ljava/lang/String;)V");
        if (setID == NULL) {
            throw std::runtime_error("Can't find the method in java: set");
        }
    }

    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    jniEnv->CallObjectMethodA(configurationObject, setID, args);
}


char *Configuration::get(char *key, char *defaultValue) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (defaultValue == NULL) {
        throw std::runtime_error("defaultValue parameter can't be NULL.");
    }

    check();

    if (getID == NULL) {
        getID = jniEnv->GetMethodID(configurationClass, "get",
             "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
        if (getID == NULL) {
            throw std::runtime_error("Can't find the method in java: get");
        }
    }

    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(defaultValue);

    jobject result = jniEnv->CallObjectMethodA(configurationObject, getID, args);
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) result, JNI_FALSE);
    jniEnv->DeleteLocalRef(result);
    return str;
}

jobject Configuration::getConfigurationObject() {
    return configurationObject;
}
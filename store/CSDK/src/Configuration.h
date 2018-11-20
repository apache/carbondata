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

class Configuration {
private:
    /**
     * configuration Class for creating object and get method id
     */
    jclass configurationClass = NULL;
    /**
     * configuration object for calling method
     */
    jobject configurationObject = NULL;
    /**
     * init id of Configuration
     */
    jmethodID initID = NULL;
    /**
     * set id of Configuration
     */
    jmethodID setID = NULL;
    /**
     * get id of Configuration
     */
    jmethodID getID = NULL;

    /**
     * check configuration class and Object
     */
    void check();

public:

    /**
     * jni env
     */
    JNIEnv *jniEnv;

    /**
    * Constructor and express the configuration
    *
    * @param env JNI env
    */
    Configuration(JNIEnv *env);

    /**
     * configure parameter, including ak,sk and endpoint
     *
     * @param key key word
     * @param value value
     */
    void set(char *key, char *value);

    /**
    * get parameter value from configure
    *
    * @param key key word
    * @param defaultValue default value
    * @return parameter value
    */
    char *get(char *key, char *defaultValue);

    /**
     * get the configuration object
     *
     * @return configuration object
     */
    jobject getConfigurationObject();
};

#include <jni.h>

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

class CarbonProperties {
private:
    /**
     * carbonProperties Class
     */
    jclass carbonPropertiesClass = NULL;

    /**
     * carbonProperties Object
     */
    jobject carbonPropertiesObject = NULL;
public:
    /**
     * jni env
     */
    JNIEnv *jniEnv = NULL;

    /**
     * Constructor of CarbonProperties
     *
     * @param env JNI env
     */
    CarbonProperties(JNIEnv *env);

    /**
     * This method will be used to add a new property
     * 
     * @param key property key
     * @param value property value
     * @return CarbonProperties object
     */
    jobject addProperty(char *key, char *value);

    /**
     * This method will be used to get the properties value
     *
     * @param key  property key
     * @return  property value
     */
    char *getProperty(char *key);

    /**
     * This method will be used to get the properties value
     * if property is not present then it will return the default value
     *
     * @param key  property key
     * @param defaultValue  property default Value
     * @return
     */
    char *getProperty(char *key, char *defaultValue);

};

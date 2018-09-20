<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
-->

# CSDK Guide

CarbonData CSDK provides C++ interface to write and read carbon file. 
CSDK use JNI to invoke java SDK in C++ code.


# CSDK Reader
This CSDK reader reads CarbonData file and carbonindex file at a given path.
External client can make use of this reader to read CarbonData files in C++ 
code and without CarbonSession.


In the carbon jars package, there exist a carbondata-sdk.jar, 
including SDK reader for CSDK.
## Quick example
```
// 1. init JVM
JavaVM *jvm;
JNIEnv *initJVM() {
    JNIEnv *env;
    JavaVMInitArgs vm_args;
    int parNum = 3;
    int res;
    JavaVMOption options[parNum];

    options[0].optionString = "-Djava.compiler=NONE";
    options[1].optionString = "-Djava.class.path=../../sdk/target/carbondata-sdk.jar";
    options[2].optionString = "-verbose:jni";
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = parNum;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;

    res = JNI_CreateJavaVM(&jvm, (void **) &env, &vm_args);
    if (res < 0) {
        fprintf(stderr, "\nCan't create Java VM\n");
        exit(1);
    }

    return env;
}

// 2. create carbon reader and read data 
// 2.1 read data from local disk
/**
 * test read data from local disk, without projection
 *
 * @param env  jni env
 * @return
 */
bool readFromLocalWithoutProjection(JNIEnv *env) {

    CarbonReader carbonReaderClass;
    carbonReaderClass.builder(env, "../resources/carbondata", "test");
    carbonReaderClass.build();

    while (carbonReaderClass.hasNext()) {
        jobjectArray row = carbonReaderClass.readNextRow();
        jsize length = env->GetArrayLength(row);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(row, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("\n");
    }
    carbonReaderClass.close();
}

// 2.2 read data from S3

/**
 * read data from S3
 * parameter is ak sk endpoint
 *
 * @param env jni env
 * @param argv argument vector
 * @return
 */
bool readFromS3(JNIEnv *env, char *argv[]) {
    CarbonReader reader;

    char *args[3];
    // "your access key"
    args[0] = argv[1];
    // "your secret key"
    args[1] = argv[2];
    // "your endPoint"
    args[2] = argv[3];

    reader.builder(env, "s3a://sdk/WriterOutput", "test");
    reader.withHadoopConf(3, args);
    reader.build();
    printf("\nRead data from S3:\n");
    while (reader.hasNext()) {
        jobjectArray row = reader.readNextRow();
        jsize length = env->GetArrayLength(row);

        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(row, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("\n");
    }

    reader.close();
}

// 3. destory JVM
    (jvm)->DestroyJavaVM();
```
Find example code at main.cpp of CSDK module

## API List
```
    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * @param tableName table name
     * @return CarbonReaderBuilder object
     */
    jobject builder(JNIEnv *env, char *path, char *tableName);

    /**
     * Configure the projection column names of carbon reader
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return CarbonReaderBuilder object
     */
    jobject projection(int argc, char *argv[]);

    /**
     *  build carbon reader with argument vector
     *  it support multiple parameter
     *  like: key=value
     *  for example: fs.s3a.access.key=XXXX, XXXX is user's access key value
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return CarbonReaderBuilder object
     **/
    jobject withHadoopConf(int argc, char *argv[]);

    /**
     * build carbonReader object for reading data
     * it support read data from load disk
     *
     * @return carbonReader object
     */
    jobject build();

    /**
     * Whether it has next row data
     *
     * @return boolean value, if it has next row, return true. if it hasn't next row, return false.
     */
    jboolean hasNext();

    /**
     * read next row from data
     *
     * @return object array of one row
     */
    jobjectArray readNextRow();

    /**
     * close the carbon reader
     *
     * @return  boolean value
     */
    jboolean close();

```

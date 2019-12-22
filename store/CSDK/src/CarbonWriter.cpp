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
#include "CarbonWriter.h"

void CarbonWriter::builder(JNIEnv *env) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    jniEnv = env;
    carbonWriter = env->FindClass("org/apache/carbondata/sdk/file/CarbonWriter");
    if (carbonWriter == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/CarbonWriter");
    }
    jmethodID carbonWriterBuilderID = env->GetStaticMethodID(carbonWriter, "builder",
        "()Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (carbonWriterBuilderID == NULL) {
        throw std::runtime_error("Can't find the method in java: carbonWriterBuilder");
    }
    carbonWriterBuilderObject = env->CallStaticObjectMethod(carbonWriter, carbonWriterBuilderID);
}

bool CarbonWriter::checkBuilder() {
    if (carbonWriterBuilderObject == NULL) {
        throw std::runtime_error("carbonWriterBuilder Object can't be NULL. Please call builder method first.");
    }
}

void CarbonWriter::outputPath(char *path) {
    if (path == NULL) {
        throw std::runtime_error("path parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "outputPath",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: outputPath");
    }
    jstring jPath = jniEnv->NewStringUTF(path);
    jvalue args[1];
    args[0].l = jPath;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
}

void CarbonWriter::sortBy(int argc, char **argv) {
    if (argc < 0) {
        throw std::runtime_error("argc parameter can't be negative.");
    }
    if (argv == NULL) {
        throw std::runtime_error("argv parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "sortBy",
        "([Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: sortBy");
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
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

/**
 * configure the schema with json style schema
 *
 * @param jsonSchema json style schema
 * @return updated CarbonWriterBuilder
 */
void CarbonWriter::withCsvInput(char *jsonSchema) {
    if (jsonSchema == NULL) {
        throw std::runtime_error("jsonSchema parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withCsvInput",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withCsvInput");
    }
    jstring jPath = jniEnv->NewStringUTF(jsonSchema);
    jvalue args[1];
    args[0].l = jPath;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
};

void CarbonWriter::withCsvInput() {
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withCsvInput",
                                             "()Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withCsvInput");
    }
    carbonWriterBuilderObject = jniEnv->CallObjectMethod(carbonWriterBuilderObject, methodID);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
};

void CarbonWriter::withHadoopConf(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withHadoopConf",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withHadoopConf");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
}

void CarbonWriter::withTableProperty(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withTableProperty",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withTableProperty");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::withLoadOption(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withLoadOption",
         "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withLoadOption");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::taskNo(long taskNo) {
    if (taskNo < 0) {
        throw std::runtime_error("taskNo parameter can't be negative.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "taskNo",
        "(J)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: taskNo");
    }
    jvalue args[1];
    args[0].j = taskNo;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::uniqueIdentifier(long timestamp) {
    if (timestamp < 1) {
        throw std::runtime_error("timestamp parameter can't be negative.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "uniqueIdentifier",
        "(J)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: uniqueIdentifier");
    }
    jvalue args[1];
    args[0].j = timestamp;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::withThreadSafe(short numOfThreads) {
    if (numOfThreads < 1) {
        throw std::runtime_error("numOfThreads parameter can't be negative.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withThreadSafe",
        "(S)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withThreadSafe");
    }
    jvalue args[1];
    args[0].s = numOfThreads;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::withBlockSize(int blockSize) {
    if (blockSize < 1) {
        throw std::runtime_error("blockSize parameter should be positive number.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withBlockSize",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withBlockSize");
    }
    jvalue args[1];
    args[0].i = blockSize;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::withBlockletSize(int blockletSize) {
    if (blockletSize < 1) {
        throw std::runtime_error("blockletSize parameter should be positive number.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withBlockletSize",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withBlockletSize");
    }
    jvalue args[1];
    args[0].i = blockletSize;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

/**
 * To set the path of carbon schema file
 * @param schemaFilePath The path of carbon schema file
 */
void CarbonWriter::withSchemaFile(char *schemaFilePath) {
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withSchemaFile",
                                             "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withSchemaFile");
    }
    jvalue args[1];
    args[0].l = jniEnv->NewStringUTF(schemaFilePath);
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::localDictionaryThreshold(int localDictionaryThreshold) {
    if (localDictionaryThreshold < 1) {
        throw std::runtime_error("localDictionaryThreshold parameter should be positive number.");
    }
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "localDictionaryThreshold",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: localDictionaryThreshold");
    }
    jvalue args[1];
    args[0].i = localDictionaryThreshold;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::enableLocalDictionary(bool enableLocalDictionary) {
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "enableLocalDictionary",
        "(Z)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: enableLocalDictionary");
    }
    jvalue args[1];
    args[0].z = enableLocalDictionary;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

void CarbonWriter::writtenBy(char *appName) {
    checkBuilder();
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "writtenBy",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: writtenBy");
    }
    jvalue args[1];
    args[0].l = jniEnv->NewStringUTF(appName);
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
}

void CarbonWriter::build() {
    checkBuilder();

    // If not add this, it will throw java.io.IOException: No FileSystem for scheme: file
    withHadoopConf("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "build",
        "()Lorg/apache/carbondata/sdk/file/CarbonWriter;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: build");
    }
    carbonWriterObject = jniEnv->CallObjectMethod(carbonWriterBuilderObject, methodID);

    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
}

bool CarbonWriter::checkWriter() {
    if (carbonWriterObject == NULL) {
        throw std::runtime_error("carbonWriter Object is NULL, Please call build first.");
    }
}

void CarbonWriter::write(jobject obj) {
    checkWriter();
    if (writeID == NULL) {
        carbonWriter = jniEnv->GetObjectClass(carbonWriterObject);
        writeID = jniEnv->GetMethodID(carbonWriter, "write", "(Ljava/lang/Object;)V");
        if (writeID == NULL) {
            throw std::runtime_error("Can't find the method in java: write");
        }
    }
    jvalue args[1];
    args[0].l = obj;
    jniEnv->CallBooleanMethodA(carbonWriterObject, writeID, args);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
};

void CarbonWriter::close() {
    checkWriter();
    jclass carbonWriter = jniEnv->GetObjectClass(carbonWriterObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriter, "close", "()V");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: close");
    }
    jniEnv->CallBooleanMethod(carbonWriterObject, methodID);
    if (jniEnv->ExceptionCheck()) {
        throw jniEnv->ExceptionOccurred();
    }
    jniEnv->DeleteLocalRef(carbonWriterBuilderObject);
    jniEnv->DeleteLocalRef(carbonWriterObject);
    jniEnv->DeleteLocalRef(carbonWriter);
}
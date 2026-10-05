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

namespace {

// This SDK embeds a JVM and keeps JNIEnv for the writer lifetime, so local
// references are not released when a C++ function returns. Temporaries go in
// a local frame. Objects stored on CarbonWriter are global references.
class JniLocalFrame {
public:
    explicit JniLocalFrame(JNIEnv *env, jint capacity) : env_(env), active_(false) {
        if (env_->PushLocalFrame(capacity) != 0) {
            throw std::runtime_error("Can't push JNI local frame.");
        }
        active_ = true;
    }

    ~JniLocalFrame() {
        if (active_) {
            env_->PopLocalFrame(NULL);
        }
    }

    jobject pop(jobject keep) {
        active_ = false;
        return env_->PopLocalFrame(keep);
    }

private:
    JniLocalFrame(const JniLocalFrame &);
    JniLocalFrame &operator=(const JniLocalFrame &);

    JNIEnv *env_;
    bool active_;
};

template <typename T>
void storeGlobalRef(JNIEnv *env, T &slot, T localRef) {
    if (localRef == NULL) {
        return;
    }
    T globalRef = static_cast<T>(env->NewGlobalRef(localRef));
    env->DeleteLocalRef(localRef);
    if (globalRef == NULL) {
        throw std::runtime_error("Failed to create JNI global reference.");
    }
    if (slot != NULL) {
        env->DeleteGlobalRef(slot);
    }
    slot = globalRef;
}

template <typename T>
void deleteGlobalRef(JNIEnv *env, T &slot) {
    if (slot != NULL) {
        env->DeleteGlobalRef(slot);
        slot = NULL;
    }
}

}  // namespace

void CarbonWriter::builder(JNIEnv *env) {
    if (env == NULL) {
        throw std::runtime_error("JNIEnv parameter can't be NULL.");
    }
    jniEnv = env;
    jclass localClass = env->FindClass("org/apache/carbondata/sdk/file/CarbonWriter");
    if (localClass == NULL) {
        throw std::runtime_error("Can't find the class in java: org/apache/carbondata/sdk/file/CarbonWriter");
    }
    storeGlobalRef(env, carbonWriter, localClass);
    jmethodID carbonWriterBuilderID = env->GetStaticMethodID(carbonWriter, "builder",
        "()Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (carbonWriterBuilderID == NULL) {
        throw std::runtime_error("Can't find the method in java: carbonWriterBuilder");
    }
    jobject localBuilder = env->CallStaticObjectMethod(carbonWriter, carbonWriterBuilderID);
    storeGlobalRef(env, carbonWriterBuilderObject, localBuilder);
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
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "outputPath",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: outputPath");
    }
    jstring jPath = jniEnv->NewStringUTF(path);
    jvalue args[1];
    args[0].l = jPath;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::sortBy(int argc, char **argv) {
    if (argc < 0) {
        throw std::runtime_error("argc parameter can't be negative.");
    }
    if (argv == NULL) {
        throw std::runtime_error("argv parameter can't be NULL.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "sortBy",
        "([Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: sortBy");
    }
    jclass objectArrayClass = jniEnv->FindClass("java/lang/String");
    if (objectArrayClass == NULL) {
        throw std::runtime_error("Can't find the class in java: java/lang/String");
    }
    jobjectArray array = jniEnv->NewObjectArray(argc, objectArrayClass, NULL);
    if (array == NULL) {
        if (jniEnv->ExceptionCheck()) {
            jthrowable exception = jniEnv->ExceptionOccurred();
            throw (jthrowable) frame.pop(exception);
        }
        throw std::runtime_error("Can't create String array for sortBy.");
    }
    for (int i = 0; i < argc; ++i) {
        jstring value = jniEnv->NewStringUTF(argv[i]);
        if (value == NULL) {
            jthrowable exception = jniEnv->ExceptionOccurred();
            throw (jthrowable) frame.pop(exception);
        }
        jniEnv->SetObjectArrayElement(array, i, value);
        jniEnv->DeleteLocalRef(value);
        if (jniEnv->ExceptionCheck()) {
            jthrowable exception = jniEnv->ExceptionOccurred();
            throw (jthrowable) frame.pop(exception);
        }
    }

    jvalue args[1];
    args[0].l = array;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
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
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withCsvInput",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withCsvInput");
    }
    jstring jPath = jniEnv->NewStringUTF(jsonSchema);
    jvalue args[1];
    args[0].l = jPath;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
};

void CarbonWriter::withCsvInput() {
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withCsvInput",
                                             "()Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withCsvInput");
    }
    jobject result = jniEnv->CallObjectMethod(carbonWriterBuilderObject, methodID);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
};

void CarbonWriter::withHadoopConf(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withHadoopConf",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withHadoopConf");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::withTableProperty(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withTableProperty",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withTableProperty");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::withLoadOption(char *key, char *value) {
    if (key == NULL) {
        throw std::runtime_error("key parameter can't be NULL.");
    }
    if (value == NULL) {
        throw std::runtime_error("value parameter can't be NULL.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withLoadOption",
         "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withLoadOption");
    }
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::taskNo(long taskNo) {
    if (taskNo < 0) {
        throw std::runtime_error("taskNo parameter can't be negative.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "taskNo",
        "(J)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: taskNo");
    }
    jvalue args[1];
    args[0].j = taskNo;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::uniqueIdentifier(long timestamp) {
    if (timestamp < 1) {
        throw std::runtime_error("timestamp parameter can't be negative.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "uniqueIdentifier",
        "(J)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: uniqueIdentifier");
    }
    jvalue args[1];
    args[0].j = timestamp;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::withThreadSafe(short numOfThreads) {
    if (numOfThreads < 1) {
        throw std::runtime_error("numOfThreads parameter can't be negative.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withThreadSafe",
        "(S)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withThreadSafe");
    }
    jvalue args[1];
    args[0].s = numOfThreads;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::withBlockSize(int blockSize) {
    if (blockSize < 1) {
        throw std::runtime_error("blockSize parameter should be positive number.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withBlockSize",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withBlockSize");
    }
    jvalue args[1];
    args[0].i = blockSize;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::withBlockletSize(int blockletSize) {
    if (blockletSize < 1) {
        throw std::runtime_error("blockletSize parameter should be positive number.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withBlockletSize",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withBlockletSize");
    }
    jvalue args[1];
    args[0].i = blockletSize;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

/**
 * To set the path of carbon schema file
 * @param schemaFilePath The path of carbon schema file
 */
void CarbonWriter::withSchemaFile(char *schemaFilePath) {
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withSchemaFile",
                                             "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: withSchemaFile");
    }
    jvalue args[1];
    args[0].l = jniEnv->NewStringUTF(schemaFilePath);
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::localDictionaryThreshold(int localDictionaryThreshold) {
    if (localDictionaryThreshold < 1) {
        throw std::runtime_error("localDictionaryThreshold parameter should be positive number.");
    }
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "localDictionaryThreshold",
        "(I)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: localDictionaryThreshold");
    }
    jvalue args[1];
    args[0].i = localDictionaryThreshold;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::enableLocalDictionary(bool enableLocalDictionary) {
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "enableLocalDictionary",
        "(Z)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: enableLocalDictionary");
    }
    jvalue args[1];
    args[0].z = enableLocalDictionary;
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::writtenBy(char *appName) {
    checkBuilder();
    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "writtenBy",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: writtenBy");
    }
    jvalue args[1];
    args[0].l = jniEnv->NewStringUTF(appName);
    jobject result = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, methodID, args);
    storeGlobalRef(jniEnv, carbonWriterBuilderObject, frame.pop(result));
}

void CarbonWriter::build() {
    checkBuilder();

    // If not add this, it will throw java.io.IOException: No FileSystem for scheme: file
    withHadoopConf("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    JniLocalFrame frame(jniEnv, 16);
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID methodID = jniEnv->GetMethodID(carbonWriterBuilderClass, "build",
        "()Lorg/apache/carbondata/sdk/file/CarbonWriter;");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: build");
    }
    jobject result = jniEnv->CallObjectMethod(carbonWriterBuilderObject, methodID);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    storeGlobalRef(jniEnv, carbonWriterObject, frame.pop(result));
}

bool CarbonWriter::checkWriter() {
    if (carbonWriterObject == NULL) {
        throw std::runtime_error("carbonWriter Object is NULL, Please call build first.");
    }
}

void CarbonWriter::write(jobject obj) {
    checkWriter();
    if (writeID == NULL) {
        JniLocalFrame frame(jniEnv, 4);
        jclass writerClass = jniEnv->GetObjectClass(carbonWriterObject);
        writeID = jniEnv->GetMethodID(writerClass, "write", "(Ljava/lang/Object;)V");
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
    JniLocalFrame frame(jniEnv, 4);
    jclass writerClass = jniEnv->GetObjectClass(carbonWriterObject);
    jmethodID methodID = jniEnv->GetMethodID(writerClass, "close", "()V");
    if (methodID == NULL) {
        throw std::runtime_error("Can't find the method in java: close");
    }
    jniEnv->CallBooleanMethod(carbonWriterObject, methodID);
    if (jniEnv->ExceptionCheck()) {
        jthrowable exception = jniEnv->ExceptionOccurred();
        throw (jthrowable) frame.pop(exception);
    }
    frame.pop(NULL);
    deleteGlobalRef(jniEnv, carbonWriterBuilderObject);
    deleteGlobalRef(jniEnv, carbonWriterObject);
    deleteGlobalRef(jniEnv, carbonWriter);
    writeID = NULL;
}

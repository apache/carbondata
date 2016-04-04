package org.carbondata.core.writer;

import java.io.Closeable;
import java.io.IOException;

/**
 * dictionary writer interface
 */
public interface CarbonDictionaryWriter extends Closeable {
    /**
     * write method that accepts one value at a time
     */
    void write(String columnValue) throws IOException;

    /**
     * this method will return the dictionary file path
     */
    String getDictionaryFilePath();

    /**
     * this method will return the dictionary offset file path
     */
    String getDictionaryMetaFilePath();
}

package org.carbondata.core.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * dictionary writer interface
 */
public interface CarbonDictionaryWriter extends Closeable {
    /**
     * write method that accepts one value at a time
     */
    void write(String value) throws IOException;

    /**
     * write method that accepts list of byte arrays as value
     */
    void write(List<byte[]> valueList) throws IOException;
}

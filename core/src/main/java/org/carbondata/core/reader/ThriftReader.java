package org.carbondata.core.reader;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonUtil;

/**
 * A simple class for reading Thrift objects (of a single type) from a fileName.
 *
 * @author Joel Meyer
 */
public class ThriftReader {
    /**
     * Thrift deserializes by taking an existing object and populating it. ThriftReader
     * needs a way of obtaining instances of the class to be populated and this interface
     * defines the mechanism by which a client provides these instances.
     */
    public static interface TBaseCreator {
        TBase create();
    }

    /**
     * buffer size
     */
    private static final int bufferSize = 2048;

    /**
     * File containing the objects.
     */
    private String fileName;

    /**
     * Used to create empty objects that will be initialized with values from the fileName.
     */
    private final TBaseCreator creator;

    /**
     * For reading the fileName.
     */
    private DataInputStream dataInputStream;

    /**
     * For reading the binary thrift objects.
     */
    private TBinaryProtocol binaryIn;

    /**
     * Constructor.
     */
    public ThriftReader(String fileName, TBaseCreator creator) {
        this.fileName = fileName;
        this.creator = creator;
    }

    /**
     * Opens the fileName for reading.
     */
    public void open() throws IOException {
        FileFactory.FileType fileType = FileFactory.getFileType(fileName);
        dataInputStream = FileFactory.getDataInputStream(fileName, fileType, bufferSize);
        binaryIn = new TBinaryProtocol(new TIOStreamTransport(dataInputStream));
    }

    /**
     * Checks if another objects is available by attempting to read another byte from the stream.
     */
    public boolean hasNext() throws IOException {
        dataInputStream.mark(1);
        int val = dataInputStream.read();
        dataInputStream.reset();
        return val != -1;
    }

    /**
     * Reads the next object from the fileName.
     */
    public TBase read() throws IOException {
        TBase t = creator.create();
        try {
            t.read(binaryIn);
        } catch (TException e) {
            throw new IOException(e);
        }
        return t;
    }

    /**
     * Close the fileName.
     */
    public void close() {
        CarbonUtil.closeStreams(dataInputStream);
    }
}

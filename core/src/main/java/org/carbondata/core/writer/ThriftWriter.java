package org.carbondata.core.writer;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonUtil;

/**
 * Simple class that makes it easy to write Thrift objects to disk.
 *
 * @author Joel Meyer
 */
public class ThriftWriter {

    /**
     * buffer size
     */
    private static final int bufferSize = 2048;

    /**
     * File to write to.
     */
    private String fileName;

    /**
     * For writing to the file.
     */
    private DataOutputStream dataOutputStream;

    /**
     * For binary serialization of objects.
     */
    private TBinaryProtocol binaryOut;

    /**
     * flag to append to existing file
     */
    private boolean append;

    /**
     * Constructor.
     */
    public ThriftWriter(String fileName, boolean append) {
        this.fileName = fileName;
        this.append = append;
    }

    /**
     * Open the file for writing.
     */
    public void open() throws IOException {
        FileFactory.FileType fileType = FileFactory.getFileType(fileName);
        dataOutputStream = FileFactory.getDataOutputStream(fileName, fileType, append, bufferSize);
        binaryOut = new TBinaryProtocol(new TIOStreamTransport(dataOutputStream));
    }

    /**
     * Write the object to disk.
     */
    public void write(TBase t) throws IOException {
        try {
            t.write(binaryOut);
            dataOutputStream.flush();
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    /**
     * Close the file stream.
     */
    public void close() {
        CarbonUtil.closeStreams(dataOutputStream);
    }
}

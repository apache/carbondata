package org.apache.carbondata.core.datastore.compression;

import java.io.IOException;

public class NoneCompressor extends AbstractCompressor {

    @Override
    public String getName() {
        return "none";
    }

    @Override
    public byte[] compressByte(byte[] unCompInput) {
        return unCompInput;
    }

    @Override
    public byte[] compressByte(byte[] unCompInput, int byteSize) {
        return unCompInput;
    }

    @Override
    public byte[] unCompressByte(byte[] compInput) {
        return compInput;
    }

    @Override
    public byte[] unCompressByte(byte[] compInput, int offset, int length) {
        return compInput;
    }

    @Override
    public long rawUncompress(byte[] input, byte[] output) throws IOException {
        throw new RuntimeException("Not implemented rawCompress for noneCompressor yet");
    }

    @Override
    public long maxCompressedLength(long inputSize) {
        return inputSize;
    }

    @Override
    public int unCompressedLength(byte[] data, int offset, int length) {
        throw new RuntimeException("Unsupported operation Exception");
    }

    @Override
    public int rawUncompress(byte[] data, int offset, int length, byte[] output) {
        throw new RuntimeException("Not implemented rawCompress for noneCompressor yet");
    }
}

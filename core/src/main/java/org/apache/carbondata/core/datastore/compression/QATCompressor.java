package org.apache.carbondata.core.datastore.compression;

import org.apache.carbondata.core.util.ByteUtil;

import java.io.IOException;

import com.intel.qat.func.QatCompressor;
import com.intel.qat.func.QatDecompressor;

public class QATCompressor extends AbstractCompressor {

    QatCompressor compressor = new QatCompressor();
    QatDecompressor decompressor = new QatDecompressor();

    @Override
    public String getName() {
        return "qat";
    }

    @Override public byte[] compressByte(byte[] unCompInput) {
        try{
            return compressor.compress(unCompInput);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override public byte[] compressByte(byte[] unCompInput, int byteSize) {
        try{
            return compressor.compress(unCompInput, byteSize);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override public byte[] unCompressByte(byte[] compInput) {
        try{
            return decompressor.decompress(compInput);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override public byte[] unCompressByte(byte[] compInput, int offset, int length){
        try{
            return decompressor.decompress(compInput, offset, length);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override public long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException{
        throw new IOException("Unsupported operation Exception");
    }

    @Override public long rawUncompress(byte[] input, byte[] output) throws IOException{
        throw new IOException("Unsupported operation Exception");
    }

    @Override public long maxCompressedLength(long inputSize){
        return inputSize;
    }

    @Override public int unCompressedLength(byte[] data, int offset, int length){
        throw new RuntimeException("Unsupported operation Exception");
    }

    @Override public int rawUncompress(byte[] compInput, int offset, int length, byte[] output){
        throw new RuntimeException("Unsupported operation Exception");
    }

    @Override public boolean supportReusableBuffer(){
        return true;
    }
}


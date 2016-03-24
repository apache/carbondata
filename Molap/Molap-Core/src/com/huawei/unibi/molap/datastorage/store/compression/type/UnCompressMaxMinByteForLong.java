package com.huawei.unibi.molap.datastorage.store.compression.type;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

public class UnCompressMaxMinByteForLong extends UnCompressMaxMinByte {

    private static Compressor<byte[]> byteCompressor =
            SnappyCompression.SnappyByteCompression.INSTANCE;

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnCompressMaxMinByteForLong.class.getName());

    @Override public UnCompressValue getNew() {
        try {
            return (UnCompressValue) clone();
        } catch (CloneNotSupportedException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        return null;
    }

    @Override public UnCompressValue compress() {

        UnCompressMaxMinByteForLong byte1 = new UnCompressMaxMinByteForLong();
        byte1.setValue(byteCompressor.compress(value));
        return byte1;
    }

    @Override public UnCompressValue uncompress(DataType dataType) {
        UnCompressValue byte1 = ValueCompressionUtil.unCompressMaxMin(dataType, dataType);
        ValueCompressonHolder.unCompress(dataType, byte1, value);
        return byte1;
    }

    @Override public UnCompressValue getCompressorObject() {
        return new UnCompressMaxMinByteForLong();
    }

    @Override public MolapReadDataHolder getValues(int decimal, Object maxValueObject) {
        long maxValue = (long) maxValueObject;
        long[] vals = new long[value.length];
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        for (int i = 0; i < vals.length; i++) {
            if (value[i] == 0) {
                vals[i] = maxValue;
            } else {
                vals[i] = maxValue - value[i];
            }
        }
        dataHolder.setReadableLongValues(vals);
        return dataHolder;
    }
}

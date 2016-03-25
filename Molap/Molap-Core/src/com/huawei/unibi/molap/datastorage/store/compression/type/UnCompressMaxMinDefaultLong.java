package com.huawei.unibi.molap.datastorage.store.compression.type;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;

public class UnCompressMaxMinDefaultLong extends UnCompressMaxMinLong {

    private static Compressor<long[]> longCompressor =
            SnappyCompression.SnappyLongCompression.INSTANCE;
    
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnCompressMaxMinDefaultLong.class.getName());

    @Override public UnCompressValue getNew() {
        try {
            return (UnCompressValue) clone();
        } catch (CloneNotSupportedException ex5) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, ex5, ex5.getMessage());
        }
        return null;
    }

    @Override public UnCompressValue compress() {
        UnCompressMaxMinByteForLong byte1 = new UnCompressMaxMinByteForLong();
        byte1.setValue(longCompressor.compress(value));
        return byte1;
    }

    @Override public byte[] getBackArrayData() {
        return ValueCompressionUtil.convertToBytes(value);
    }

    @Override public UnCompressValue getCompressorObject() {
        return new UnCompressMaxMinByteForLong();
    }

    @Override public MolapReadDataHolder getValues(int decimal, Object maxValueObject) {
        long maxValue = (long) maxValueObject;
        long[] vals = new long[value.length];
        MolapReadDataHolder dataHolderInfoObj = new MolapReadDataHolder();
        for (int i = 0; i < vals.length; i++) {
            if (value[i] == 0) {
                vals[i] = maxValue;
            } else {
                vals[i] = maxValue - value[i];
            }

        }
        dataHolderInfoObj.setReadableLongValues(vals);
        return dataHolderInfoObj;
    }

}

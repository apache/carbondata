package com.huawei.unibi.molap.datastorage.store.compression.type;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;

public class UnCompressDefaultLong extends UnCompressNoneLong {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnCompressDefaultLong.class.getName());

    public ValueCompressonHolder.UnCompressValue getNew() {
        try {
            return (ValueCompressonHolder.UnCompressValue) clone();
        } catch (CloneNotSupportedException clnNotSupportedExc) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, clnNotSupportedExc,
                    clnNotSupportedExc.getMessage());
        }
        return null;
    }

    @Override public MolapReadDataHolder getValues(int decimal, Object maxValueObject) {
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        long[] vals = new long[value.length];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = value[i];
        }
        dataHolder.setReadableLongValues(vals);
        return dataHolder;
    }

}

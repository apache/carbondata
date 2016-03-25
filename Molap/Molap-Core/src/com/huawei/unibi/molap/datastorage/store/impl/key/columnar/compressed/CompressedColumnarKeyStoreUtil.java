package com.huawei.unibi.molap.datastorage.store.impl.key.columnar.compressed;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;

public final class CompressedColumnarKeyStoreUtil {

    private CompressedColumnarKeyStoreUtil() {

    }

    public static void mapColumnIndexWithKeyColumnarKeyBlockData(byte[] columnarKeyBlockData,
            ColumnarKeyStoreMetadata columnarKeyStoreMetadata) {
        Map<Integer, byte[]> mapOfColumnarKeyBlockData = new HashMap<Integer, byte[]>(50);
        ByteBuffer directSurrogateKeyStoreDataHolder =
                ByteBuffer.allocate(columnarKeyBlockData.length);
        directSurrogateKeyStoreDataHolder.put(columnarKeyBlockData);
        directSurrogateKeyStoreDataHolder.flip();
        int row = -1;
        while (directSurrogateKeyStoreDataHolder.hasRemaining()) {
            short dataLength = directSurrogateKeyStoreDataHolder.getShort();
            byte[] directSurrKeyData = new byte[dataLength];
            directSurrogateKeyStoreDataHolder.get(directSurrKeyData);
            mapOfColumnarKeyBlockData.put(++row, directSurrKeyData);
        }
        columnarKeyStoreMetadata.setDirectSurrogateKeyMembers(mapOfColumnarKeyBlockData);

    }

    public static ColumnarKeyStoreDataHolder createColumnarKeyStoreMetadataForHCDims(int blockIndex,
            byte[] columnarKeyBlockData, int[] columnKeyBlockIndex,
            int[] columnKeyBlockReverseIndex, ColumnarKeyStoreInfo columnarStoreInfo) {
        ColumnarKeyStoreMetadata columnarKeyStoreMetadata;
        columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
        columnarKeyStoreMetadata.setDirectSurrogateColumn(true);
        columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
        columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
        columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
        columnarKeyStoreMetadata.setUnCompressed(true);
        mapColumnIndexWithKeyColumnarKeyBlockData(columnarKeyBlockData, columnarKeyStoreMetadata);
        ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders =
                new ColumnarKeyStoreDataHolder(columnarKeyBlockData, columnarKeyStoreMetadata);
        return columnarKeyStoreDataHolders;
    }

    public static boolean isHighCardinalityBlock(int[] directSurrogates, int blockIndex) {
        if (null != directSurrogates) {
            for (int directSurrogateIndex : directSurrogates) {
                if (directSurrogateIndex == blockIndex) {
                    return true;
                }
            }
        }
        return false;
    }
}

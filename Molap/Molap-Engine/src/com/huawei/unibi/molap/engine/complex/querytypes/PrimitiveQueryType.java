package com.huawei.unibi.molap.engine.complex.querytypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.util.DataTypeConverter;
import com.huawei.unibi.molap.engine.util.QueryExecutorUtility;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.olap.SqlStatement;

public class PrimitiveQueryType implements GenericQueryType {

	private int index;
	
	private String name;
	private String parentname;
	
	private int keySize;
	
	private int blockIndex;
	
	private SqlStatement.Type dataType;
	
	public PrimitiveQueryType(String name, String parentname, int blockIndex, SqlStatement.Type dataType)
	{
		this.name = name;
		this.parentname = parentname;
		this.blockIndex = blockIndex;
		this.dataType = dataType;
	}
	
	@Override
	public void addChildren(GenericQueryType children) {

	}

	@Override
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setParentname(String parentname) {
		this.parentname = parentname;
		
	}

	@Override
	public String getParentname() {
		return parentname;
	}
	
	@Override
	public void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild) {

	}

	@Override
	public int getSurrogateIndex() {
		return index;
	}

	@Override
	public void setSurrogateIndex(int surrIndex) {
		index = surrIndex;
	}
	
	@Override
    public int getBlockIndex()
    {
        return blockIndex;
    }
    
    @Override
    public void setBlockIndex(int blockIndex)
    {
        this.blockIndex = blockIndex;
    }

	@Override
	public int getColsCount() {
		return 1;
	}

	@Override
    public void parseBlocksAndReturnComplexColumnByteArray(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder, int rowNumber, DataOutputStream dataOutputStream) throws IOException
    {
        byte[] currentVal = new byte[columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize()];
        if(!columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().isSorted())
        {
            System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
                    columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
                    .getColumnReverseIndex()[rowNumber]
                            * columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().
                            getEachRowSize(), currentVal, 0, columnarKeyStoreDataHolder[blockIndex].
                            getColumnarKeyStoreMetadata().getEachRowSize());
        }
        else
        {
            System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(), rowNumber
                    * columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize(), 
                    currentVal, 0, columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize());
        }
        dataOutputStream.write(currentVal);
    }
	
	@Override
    public void setKeySize(int[] keyBlockSize)
    {
        this.keySize = keyBlockSize[this.blockIndex];        
    }
	
	@Override
    public Object getDataBasedOnDataTypeFromSurrogates(List<InMemoryCube> slices, ByteBuffer surrogateData, Dimension[] dimensions)
    {
	    byte[] data = new byte[keySize];
        surrogateData.get(data);
        String memberData = QueryExecutorUtility.getMemberBySurrogateKey(dimensions[blockIndex], new BigInteger(data).intValue(), slices).toString();
        Object actualData = DataTypeConverter.getDataBasedOnDataType(
                memberData.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL) ? null : memberData,
                        dimensions[blockIndex].getDataType());
        if(dimensions[blockIndex].getDataType() == SqlStatement.Type.STRING)
        {
            byte[] dataBytes = ((String)actualData).getBytes(Charset.defaultCharset());
            return UTF8String.fromBytes(dataBytes);
        }
        return actualData;
    }
	
	@Override
    public void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput) throws IOException
    {
	    
//	    dataOutput.write();
    }
	
	@Override
    public DataType getSchemaType()
    {
	    switch(dataType)
        {
            case INT:
                return new IntegerType();
            case DOUBLE:
                return new DoubleType();
            case LONG:
                return new LongType();
            case BOOLEAN:
                return new BooleanType();
            case TIMESTAMP:
                return new TimestampType();
            default:
                return new StringType();
        }
    }

    @Override
    public void setKeyOrdinalForQuery(int keyOrdinalForQuery)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getKeyOrdinalForQuery()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void fillRequiredBlockData(BlockDataHolder blockDataHolder)
    {
        if(null==blockDataHolder.getColumnarKeyStore()[blockIndex])
        {
            blockDataHolder.getColumnarKeyStore()[blockIndex] = blockDataHolder
                    .getLeafDataBlock().getColumnarKeyStore(blockDataHolder.getFileHolder(),
                            blockIndex,
                            false);
        }
        else
        {
            if(!blockDataHolder.getColumnarKeyStore()[blockIndex]
                    .getColumnarKeyStoreMetadata().isUnCompressed())
            {
                blockDataHolder.getColumnarKeyStore()[blockIndex].unCompress();
            }
        }
    }
}

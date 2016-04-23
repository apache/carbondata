package org.carbondata.query.carbon.result.comparator;

import java.util.Comparator;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

public class VariableLengthKeyResultComparator implements
		Comparator<ByteArrayWrapper> {

	private byte sortOrder;

	private int noDictionaryColumnIndex;
	
	private DataType dataType;

	public VariableLengthKeyResultComparator(byte sortOrder,
			int noDictionaryColumnIndex, DataType dataType) {
		this.sortOrder = sortOrder;
		this.noDictionaryColumnIndex = noDictionaryColumnIndex;
		this.dataType=dataType;
	}

	@Override
	public int compare(ByteArrayWrapper byteArrayWrapper1,
			ByteArrayWrapper byteArrayWrapper2) {
		byte[] noDictionaryKeys1 = byteArrayWrapper1
				.getNoDictionaryKeyByIndex(noDictionaryColumnIndex);
		Object dataBasedOnDataType1 = DataTypeUtil.getDataBasedOnDataType(new String(noDictionaryKeys1), dataType);
		byte[] noDictionaryKeys2 = byteArrayWrapper2
				.getNoDictionaryKeyByIndex(noDictionaryColumnIndex);
		Object dataBasedOnDataType2 = DataTypeUtil.getDataBasedOnDataType(new String(noDictionaryKeys2), dataType);
		int cmp = 0;
		cmp =DataTypeUtil.compareBasedOnDatatYpe(dataBasedOnDataType1,dataBasedOnDataType2,dataType);
		if (sortOrder == 1) {
			cmp = cmp * -1;
		}
		return cmp;
	}

}

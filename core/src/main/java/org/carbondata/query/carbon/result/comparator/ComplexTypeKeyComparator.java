package org.carbondata.query.carbon.result.comparator;

import java.util.Comparator;

import org.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

public class ComplexTypeKeyComparator implements Comparator<ByteArrayWrapper> {

	private byte sortOrder;

	private int complexTypeKeyIndex;

	public ComplexTypeKeyComparator(byte sortOrder, int complexTypeKeyIndex) {
		this.sortOrder = sortOrder;
		this.complexTypeKeyIndex = complexTypeKeyIndex;
	}

	@Override
	public int compare(ByteArrayWrapper byteArrayWrapper1,
			ByteArrayWrapper byteArrayWrapper2) {
		byte[] noDictionaryKeys1 = byteArrayWrapper1
				.getComplexTypeByIndex(complexTypeKeyIndex);
		byte[] noDictionaryKeys2 = byteArrayWrapper2
				.getComplexTypeByIndex(complexTypeKeyIndex);
		int cmp = 0;
		cmp = UnsafeComparer.INSTANCE.compareTo(noDictionaryKeys1,
				noDictionaryKeys2);
		if (sortOrder == 1) {
			cmp = cmp * -1;
		}
		return cmp;
	}

}

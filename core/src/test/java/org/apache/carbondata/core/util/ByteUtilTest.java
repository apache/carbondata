/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.util;

import junit.framework.TestCase;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;


/**
 * This test will test the functionality of the Byte Util
 * for the comparision of 2 byte buffers
 */
public class ByteUtilTest extends TestCase {

    String dimensionValue1 = "aaaaaaaa1235";
    String dimensionValue2 = "aaaaaaaa1234";
    private ByteBuffer buff1;
    private ByteBuffer buff2;

    /**
     * This method will form one single byte [] for all the high card dims.
     *
     * @param byteBufferArr
     * @return
     */
    public static byte[] packByteBufferIntoSingleByteArray(
            ByteBuffer[] byteBufferArr) {
        // for empty array means there is no data to remove dictionary.
        if (null == byteBufferArr || byteBufferArr.length == 0) {
            return null;
        }
        int noOfCol = byteBufferArr.length;
        short toDetermineLengthOfByteArr = 2;
        short offsetLen = (short) (noOfCol * 2 + toDetermineLengthOfByteArr);
        int totalBytes = calculateTotalBytes(byteBufferArr) + offsetLen;

        ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

        // write the length of the byte [] as first short
        buffer.putShort((short) (totalBytes - toDetermineLengthOfByteArr));
        // writing the offset of the first element.
        buffer.putShort(offsetLen);

        // prepare index for byte []
        for (int index = 0; index < byteBufferArr.length - 1; index++) {
            ByteBuffer individualCol = byteBufferArr[index];
            // short lengthOfbytes = individualCol.getShort();
            int noOfBytes = individualCol.capacity();

            buffer.putShort((short) (offsetLen + noOfBytes));
            offsetLen += noOfBytes;
            individualCol.rewind();
        }

        // put actual data.
        for (int index = 0; index < byteBufferArr.length; index++) {
            ByteBuffer individualCol = byteBufferArr[index];
            buffer.put(individualCol.array());
        }

        buffer.rewind();
        return buffer.array();

    }

    /**
     * To calculate the total bytes in byte Buffer[].
     *
     * @param byteBufferArr
     * @return
     */
    private static int calculateTotalBytes(ByteBuffer[] byteBufferArr) {
        int total = 0;
        for (int index = 0; index < byteBufferArr.length; index++) {
            total += byteBufferArr[index].capacity();
        }
        return total;
    }

    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testLessThan() {
        dimensionValue1 = "aaaaa6aa1235";
        dimensionValue2 = "aaaaa5aa1234";

        prepareBuffers();
        assertFalse(UnsafeComparer.INSTANCE.compareTo(buff1, buff2) < 0);
    }

    @Test
    public void testEqualToCase() {
        dimensionValue1 = "aaaaaaaa1234";
        dimensionValue2 = "aaaaaaaa1234";

        prepareBuffers();
        assertTrue(UnsafeComparer.INSTANCE.compareTo(buff1, buff2) == 0);
    }

    @Test
    public void testLessThanInBoundaryCondition() {
        dimensionValue1 = "aaaaaaaa12341";
        dimensionValue2 = "aaaaaaaa12344";

        prepareBuffers();
        assertTrue(UnsafeComparer.INSTANCE.compareTo(buff1, buff2) < 0);
    }
    

	@Test
	public void testBinaryRangeSearch() {

		byte[] dataChunk = new byte[10];

		dataChunk = "abbcccddddeffgggh".getBytes();
		byte[] key = new byte[1];
		int[] range;

		key[0] = Byte.valueOf("97");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(0, range[0]);
		this.assertEquals(0, range[1]);

		key[0] = Byte.valueOf("104");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(16, range[0]);
		this.assertEquals(16, range[1]);

		key[0] = Byte.valueOf("101");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(10, range[0]);
		this.assertEquals(10, range[1]);

		key[0] = Byte.valueOf("99");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(3, range[0]);
		this.assertEquals(5, range[1]);

		dataChunk = "ab".getBytes();

		key[0] = Byte.valueOf("97");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(0, range[0]);
		this.assertEquals(0, range[1]);

		key[0] = Byte.valueOf("98");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(1, range[0]);
		this.assertEquals(1, range[1]);

		dataChunk = "aabb".getBytes();

		key[0] = Byte.valueOf("97");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(0, range[0]);
		this.assertEquals(1, range[1]);

		key[0] = Byte.valueOf("98");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(2, range[0]);
		this.assertEquals(3, range[1]);

		dataChunk = "a".getBytes();

		key[0] = Byte.valueOf("97");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(0, range[0]);
		this.assertEquals(0, range[1]);

		dataChunk = "aa".getBytes();

		key[0] = Byte.valueOf("97");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(0, range[0]);
		this.assertEquals(1, range[1]);

		dataChunk = "aabbbbbbbbbbcc".getBytes();
		key[0] = Byte.valueOf("98");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length, key);
		this.assertEquals(2, range[0]);
		this.assertEquals(11, range[1]);

	}

	@Test
	public void testBinaryRangeSearchLengthTwo() {

		byte[] dataChunk = new byte[10];

		dataChunk = "aabbbbbbbbbbcc".getBytes();
		byte[] keyWord = new byte[2];
		int[] range;

		keyWord[0] = Byte.valueOf("98");
		keyWord[1] = Byte.valueOf("98");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length ,
				keyWord);
		this.assertEquals(1, range[0]);
		this.assertEquals(5, range[1]);

		keyWord[0] = Byte.valueOf("97");
		keyWord[1] = Byte.valueOf("97");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length ,
				keyWord);
		this.assertEquals(0, range[0]);
		this.assertEquals(0, range[1]);

		keyWord[0] = Byte.valueOf("99");
		keyWord[1] = Byte.valueOf("99");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length ,
				keyWord);
		this.assertEquals(6, range[0]);
		this.assertEquals(6, range[1]);

	}

	@Test
	public void testBinaryRangeSearchLengthThree() {

		byte[] dataChunk = new byte[10];

		dataChunk = "aaabbbbbbbbbccc".getBytes();
		byte[] keyWord = new byte[3];
		int[] range;

		keyWord[0] = Byte.valueOf("98");
		keyWord[1] = Byte.valueOf("98");
		keyWord[2] = Byte.valueOf("98");
		range = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeSearch(dataChunk, 0, dataChunk.length ,
				keyWord);
		this.assertEquals(1, range[0]);
		this.assertEquals(3, range[1]);

	}

	@Test
	public void testbinaryRangeBoundarySearchLengthTwo() {

		byte[] dataChunk = new byte[10];
		byte[] keyWord = new byte[2];
		int[] expectRangeIndex = new int[2];
		int[] lowRangeIndex = new int[2];
		int[] midRangeIndex = new int[2];
		int[] highRangeIndex = new int[2];

		// 0-4 5-10 11-17
		dataChunk = "abababababacacacacacacadadadadadadad".getBytes();

		keyWord[0] = Byte.valueOf("97");
		keyWord[1] = Byte.valueOf("100");
		expectRangeIndex[0] = 11;
		expectRangeIndex[1] = 17;
		lowRangeIndex[0] = 0;
		lowRangeIndex[1] = 11;
		midRangeIndex[0] = 11;
		midRangeIndex[1] = 17;
		highRangeIndex[0] = 17;
		highRangeIndex[1] = 17;

		testRangeboundary(dataChunk, keyWord, expectRangeIndex, lowRangeIndex, midRangeIndex, highRangeIndex);

		keyWord[0] = Byte.valueOf("97");
		keyWord[1] = Byte.valueOf("98");
		expectRangeIndex[0] = 0;
		expectRangeIndex[1] = 4;
		lowRangeIndex[0] = 0;
		lowRangeIndex[1] = 0;
		midRangeIndex[0] = 0;
		midRangeIndex[1] = 4;
		highRangeIndex[0] = 4;
		highRangeIndex[1] = 17;

		testRangeboundary(dataChunk, keyWord, expectRangeIndex, lowRangeIndex, midRangeIndex, highRangeIndex);

		keyWord[0] = Byte.valueOf("97");
		keyWord[1] = Byte.valueOf("99");
		expectRangeIndex[0] = 5;
		expectRangeIndex[1] = 10;
		lowRangeIndex[0] = 0;
		lowRangeIndex[1] = 5;
		midRangeIndex[0] = 5;
		midRangeIndex[1] = 10;
		highRangeIndex[0] = 10;
		highRangeIndex[1] = 17;

		testRangeboundary(dataChunk, keyWord, expectRangeIndex, lowRangeIndex, midRangeIndex, highRangeIndex);
		
		
		// 0-4
		dataChunk = "aaaaaaaaaa".getBytes();
		keyWord[0] = Byte.valueOf("97");
		keyWord[1] = Byte.valueOf("97");
		expectRangeIndex[0] = 0;
		expectRangeIndex[1] = 4;
		lowRangeIndex[0] = 0;
		lowRangeIndex[1] = 0;
		midRangeIndex[0] = 0;
		midRangeIndex[1] = 4;
		highRangeIndex[0] = 4;
		highRangeIndex[1] = 4;

		testRangeboundary(dataChunk, keyWord, expectRangeIndex, lowRangeIndex, midRangeIndex, highRangeIndex);

	}

	/**
	 * use to test a specific key's range bound in sorted byte array
	 *
	 * @param dataChunk
	 *            is a sorted byte array according to filter value's
	 * @param keyWord
	 *            is a specific value
	 * @param expectRangeIndex
	 * @param lowRangeIndex
	 * @param midRangeIndex
	 * @param highRangeIndex
	 * @return
	 */
	private void testRangeboundary(byte[] dataChunk, byte[] keyWord, int[] expectRangeIndex, int[] lowRangeIndex,
			int[] midRangeIndex, int[] highRangeIndex) {
		for (int low = lowRangeIndex[0]; low <= lowRangeIndex[1]; low++) {
			for (int mid = midRangeIndex[0]; mid <= midRangeIndex[1]; mid++) {
				for (int high = highRangeIndex[0]; high <= highRangeIndex[1]; high++) {
//					System.out.print("");
//					System.out.print("low: " + low);
//					System.out.print(" mid: " + mid);
//					System.out.println(" high: " + high);
					int boundIndex;
					// lower limit
					boundIndex = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeBoundarySearch(dataChunk, low, mid, keyWord, false);
					this.assertEquals(expectRangeIndex[0], boundIndex);
					// upper limit
					boundIndex = ByteUtil.UnsafeComparer.INSTANCE.binaryRangeBoundarySearch(dataChunk, mid, high, keyWord, true);
					this.assertEquals(expectRangeIndex[1], boundIndex);
				}
			}
		}
	}
    

    /**
     * This will prepare the byte buffers in the required format for comparision.
     */
    private void prepareBuffers() {
        ByteBuffer[] out1 = new ByteBuffer[1];
        ByteBuffer buffer = ByteBuffer.allocate(dimensionValue1.length());
        buffer.put(dimensionValue1.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
        buffer.rewind();
        out1[0] = buffer;


        ByteBuffer[] out2 = new ByteBuffer[1];

        ByteBuffer buffer2 = ByteBuffer.allocate(dimensionValue2.length());
        buffer2.put(dimensionValue2.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
        buffer2.rewind();
        out2[0] = buffer2;

        byte[] arr1 = packByteBufferIntoSingleByteArray(out1);
        byte[] arr2 = packByteBufferIntoSingleByteArray(out2);

        buff1 = ByteBuffer.wrap(arr1);

        buff1.position(4);
        buff1.limit(buff1.position() + dimensionValue1.length());

        buff2 = ByteBuffer.wrap(arr2);
        buff2.position(4);
        buff2.limit(buff2.position() + dimensionValue2.length());
    }

}

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

package com.huawei.unibi.molap.groupby;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.dataprocessor.manager.MolapDataProcessorManager;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.AbstractMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CustomAggregatorHelper;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.groupby.exception.MolapGroupByException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.util.*;

public class MolapAutoAggGroupBy {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapAutoAggGroupBy.class.getName());

    /**
     * decimalPointers
     */
    private final byte decimalPointers = Byte.parseByte(MolapProperties.getInstance()
            .getProperty(MolapCommonConstants.MOLAP_DECIMAL_POINTERS,
                    MolapCommonConstants.MOLAP_DECIMAL_POINTERS_DEFAULT));

    /**
     * key array index
     */
    protected int keyIndex;

    /**
     * aggregate type
     */
    protected String[] aggType;

    /**
     * aggClassName
     */
    protected String[] aggClassName;

    /**
     * previous row key
     */
    protected byte[] prvKey;

    /**
     * max value for each measure
     */
    protected Object[] maxValue;

    /**
     * min value for each measure
     */
    protected Object[] minValue;

    /**
     * decimal length of each measure
     */
    protected int[] decimalLength;

    /**
     * uniqueValue
     */
    protected Object[] uniqueValue;

    /**
     * max value for each measure
     */
    protected char[] type;

    /**
     * channel
     */
    protected DataOutputStream writeStream;

    /**
     * tmpFile
     */
    protected File tmpFile;

    /**
     * readingStream
     */
    protected DataInputStream readingStream;

    /**
     * numberOfEntries
     */
    protected long numberOfEntries;

    /**
     * readCounter
     */
    protected long readCounter;

    /**
     * mdKeyLength
     */
    protected int mdKeyLength;

    /**
     * storeLocation
     */
    protected String storeLocation;

    /**
     * aggregators
     */
    protected MeasureAggregator[] aggregators;

    /**
     * cubeUniqueName
     */
    protected String cubeUniqueName;

    /**
     * tableName
     */
    protected String tableName;

    /**
     * isFirst
     */
    protected boolean isFirst = true;

    /**
     * schemaName
     */
    protected String schemaName;

    /**
     * cubeName
     */
    protected String cubeName;

    /**
     * bufferSize
     */
    protected int bufferSize;

    /**
     * factKetGenerator
     */
    protected KeyGenerator factKetGenerator;
    /**
     * isNotNullValue
     */
    protected boolean[] isNotNullValue;
    /**
     * mergedMinValue
     */
    protected Object[] mergedMinValue;
    /**
     * customAggHelper
     */
    private CustomAggregatorHelper customAggHelper;
    private int currentRestructNumber;

    public MolapAutoAggGroupBy(String[] aggType, String[] aggClassName, String schemaName,
            String cubeName, String tableName, int[] factDims, String extension,
            int currentRestructNum) {
        this.keyIndex = aggType.length;
        this.aggType = aggType;
        this.aggClassName = aggClassName;
        this.schemaName = schemaName;
        this.cubeName = cubeName;
        this.tableName = tableName;
        cubeUniqueName = schemaName + '_' + cubeName;
        this.currentRestructNumber = currentRestructNum;
        initialiseMaxMinDecimal(extension);
        if (null != factDims) {
            factKetGenerator = KeyGeneratorFactory.getKeyGenerator(factDims);
        }
        customAggHelper = new CustomAggregatorHelper();

    }

    /**
     * below method will be used to initialise the max min decimal
     */
    private void initialiseMaxMinDecimal(String extension) {
        this.maxValue = new Object[this.aggType.length];
        this.minValue = new Object[this.aggType.length];
        this.decimalLength = new int[this.aggType.length];
        this.uniqueValue = new Object[this.aggType.length];
        for (int i = 0; i < this.aggType.length; i++) {
            maxValue[i] = -Double.MAX_VALUE;
            minValue[i] = Double.MAX_VALUE;
            decimalLength[i] = 0;
        }
        this.type = new char[this.aggType.length];
        Arrays.fill(type, 'n');
        for (int i = 0; i < this.aggType.length; i++) {
            if (aggType[i].equals(MolapCommonConstants.CUSTOM) || aggType[i]
                    .equals(MolapCommonConstants.DISTINCT_COUNT)) {
                this.type[i] = 'c';
            }
        }
    }

    /**
     * Below method will be used to initialize
     *
     * @throws MolapGroupByException any problem while initializing
     * @throws MolapGroupByException
     */
    protected void initialize() throws MolapGroupByException {
        updateSortTempFileLocation(schemaName, cubeName);
        this.tmpFile = new File(this.storeLocation + File.separator + tableName + System.nanoTime()
                + ".groubyfile");
        this.bufferSize = Integer.parseInt(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.MOLAP_SORT_FILE_WRITE_BUFFER_SIZE,
                        MolapCommonConstants.MOLAP_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE));
        try {
            // open output stream on temop file
            this.writeStream = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(tmpFile), bufferSize));
        } catch (FileNotFoundException e) {
            throw new MolapGroupByException("Problem while creating group by temp file", e);
        }
    }

    /**
     * This will be used to get the sort temo location
     *
     * @throws MolapGroupByException
     */
    private void updateSortTempFileLocation(String schemaName, String cubeName)
            throws MolapGroupByException {
        // get the base location
        String tempLocationKey = schemaName + '_' + cubeName;
        String baseLocation = MolapProperties.getInstance()
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        // get the temp file location
        this.storeLocation = baseLocation + File.separator + schemaName + File.separator + cubeName
                + File.separator + MolapCommonConstants.GROUP_BY_TEMP_FILE_LOCATION + File.separator
                + this.tableName;
        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "temp file location" + this.storeLocation);

        // if check point is not enabled then delete if any older file exists in
        // sort temp folder
        deleteGroupByTempLocationIfExists();
        // create new sort temp directory
        if (!new File(this.storeLocation).mkdirs()) {
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Sort Temp Location Already Exists");
        }
    }

    /**
     * This method will be used to delete sort temp location is it is exites
     *
     * @throws MolapGroupByException
     */
    private void deleteGroupByTempLocationIfExists() throws MolapGroupByException {
        // create new tem file location where this class
        // will write all the temp files
        File file = new File(this.storeLocation);
        if (file.exists()) {
            try {
                MolapUtil.deleteFoldersAndFiles(file);
            } catch (MolapUtilException e) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
                throw new MolapGroupByException(e);
            }
        }
    }

    /**
     * Below method will be used to add new row
     *
     * @param row
     */
    protected void addNewRow(Object[] row) {
        for (int i = 0; i < aggregators.length; i++) {
            if (null != row[i]) {
                this.isNotNullValue[i] = true;
                double value = (Double) row[i];
                aggregators[i].agg(value);
            }
        }
        prvKey = (byte[]) row[this.keyIndex];
        calculateMaxMinUnique();
    }

    /**
     * This method will be used to add new row it will check if new row and
     * previous row key is same then it will merger the measure values, else it
     * return the previous row
     *
     * @param row new row
     * @return previous row
     * @throws MolapGroupByException
     */
    public void add(Object[] row) throws MolapGroupByException {
        if (isFirst) {
            final Object dataProcessingLockObject = MolapDataProcessorManager.getInstance()
                    .getDataProcessingLockObject(this.schemaName + '_' + this.cubeName);
            synchronized (dataProcessingLockObject) {
                isFirst = false;
                this.mdKeyLength = ((byte[]) row[this.keyIndex]).length;

                initialize();
                initialiseAggegators();
                addNewRow(row);
            }
            return;
        }
        if (MolapDataProcessorUtil.compare(prvKey, (byte[]) row[this.keyIndex]) == 0) {
            updateMeasureValue(row);
        } else {
            writeDataToFile();
            initialiseAggegators();
            addNewRow(row);
        }
    }

    private void initialiseAggegators() {
        aggregators = AggUtil.getAggregators(Arrays.asList(this.aggType),
                Arrays.asList(this.aggClassName), false, factKetGenerator, cubeUniqueName,
                mergedMinValue, this.type);
        isNotNullValue = new boolean[this.aggType.length];
        for (int i = 0; i < aggType.length; i++) {
            if (aggType[i].equals(MolapCommonConstants.CUSTOM)) {
                ((AbstractMeasureAggregator) aggregators[i]).setCubeName(cubeName);
                ((AbstractMeasureAggregator) aggregators[i]).setSchemaName(schemaName);
                ((AbstractMeasureAggregator) aggregators[i]).setDataLoadRequest(true);
                ((AbstractMeasureAggregator) aggregators[i]).setAggregatorHelper(customAggHelper);
                isNotNullValue[i] = true;
            }
        }
    }

    /**
     * This method will be used to write the aggregated data to file For measure
     * value first it will write first boolean whether it is null or not and if
     * it not null then it will write the measure value Null check is not
     * required for mdkey because mdkey cannot be null if it null then check in
     * previous step
     *
     * @throws MolapGroupByException problem while writing
     */
    private void writeDataToFile() throws MolapGroupByException {
        try {
            byte[] byteArray = null;
            for (int i = 0; i < type.length; i++) {
                if (type[i] != 'c') {
                    if (isNotNullValue[i]) {
                        writeStream.write(1);
                        switch (type[i]) {
                        case 'l':

                            writeStream.writeLong(aggregators[i].getLongValue());
                            break;
                        case 'b':

                            writeStream.writeUTF(aggregators[i].getBigDecimalValue().toString());
                            break;
                        default:
                            writeStream.writeDouble(aggregators[i].getDoubleValue());
                        }
                    } else {
                        writeStream.write(0);
                    }
                } else {
                    byteArray = aggregators[i].getByteArray();
                    writeStream.writeInt(byteArray.length);
                    writeStream.write(byteArray);
                }
            }
            // writing the mdkey , mdkey cannnot be null if it is null then we
            // need to check in previous step
            writeStream.write(prvKey);
            this.numberOfEntries++;
        } catch (IOException e) {
            throw new MolapGroupByException("Problem while writing file", e);
        }
    }

    /**
     * This method will be used to start reading
     *
     * @throws MolapGroupByException problem in creating the input stream
     */
    public void initiateReading(String basestoreLocation, String tableName)
            throws MolapGroupByException {
        if (isFirst) {
            return;
        }
        // write the last row
        writeDataToFile();
        // close open out stream
        MolapUtil.closeStreams(writeStream);
        String metaDataFileName = MolapCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT;
        String measureMetaDataFileLocation = basestoreLocation + metaDataFileName;
        File file = new File(measureMetaDataFileLocation);
        try {
            MolapUtil.deleteFoldersAndFiles(file);
        } catch (MolapUtilException e1) {
            throw new MolapGroupByException("Problem while deleting the measure meta data file ",
                    e1);
        }
        try {
            // creating reading strem
            readingStream = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(tmpFile), bufferSize));
        } catch (FileNotFoundException e) {
            throw new MolapGroupByException("Problem while getting the ", e);
        }
    }

    /**
     * Below method will be used to check whether any more records are present
     * in the file to read or not
     *
     * @return more records are present
     */
    public boolean hasNext() {
        return readCounter < numberOfEntries;
    }

    /**
     * Below method will be used to read the record from file
     *
     * @return record
     * @throws MolapGroupByException any problem while reading
     */
    public Object[] next() throws MolapGroupByException {
        // return row will be of total number of measure + one mdkey
        Object[] outRow = new Object[type.length + 1];
        byte[] byteArray = null;
        int readInt = -1;
        try {
            // reading first all the measure value from file
            for (int i = 0; i < this.type.length; i++) {
                if (type[i] != 'c') {
                    if (readingStream.readByte() == 1) {
                        outRow[i] = readingStream.readDouble();
                    }
                } else {
                    readInt = readingStream.readInt();
                    ByteBuffer buffer =
                            ByteBuffer.allocate(MolapCommonConstants.INT_SIZE_IN_BYTE + readInt);
                    buffer.putInt(readInt);
                    byteArray = new byte[readInt];
                    if (readingStream.read(byteArray) < 0) {
                        LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                "Problme while reading the Custom Measure.");
                    }
                    buffer.put(byteArray);
                    buffer.rewind();
                    byteArray = new byte[MolapCommonConstants.INT_SIZE_IN_BYTE + readInt];
                    buffer.get(byteArray);
                    outRow[i] = byteArray;
                }
            }
            // reading the mdkey
            byteArray = new byte[mdKeyLength];

            if (readingStream.read(byteArray) < 0) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Problme while reading the Mdkey.");
            }
            // setting the mdkey
            outRow[this.aggType.length] = byteArray;
            // increment the read counter;
            this.readCounter++;
        } catch (IOException e) {
            throw new MolapGroupByException("Problem while reading the groupss temp file ", e);
        }
        // return row
        return outRow;
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     *
     * @param row row
     */
    protected void updateMeasureValue(Object[] row) {
        for (int i = 0; i < aggregators.length; i++) {
            if (null != row[i]) {
                double value = (Double) row[i];
                aggregators[i].agg(value);
            }
        }
        calculateMaxMinUnique();
    }

    /**
     * This method will be used to update the max value for each measure
     */
    protected void calculateMaxMinUnique() {
        for (int i = 0; i < aggregators.length; i++) {
            int num;
            if (isNotNullValue[i]) {
                switch (type[i]) {
                case 'l':
                    long valueL = aggregators[i].getLongValue();
                    maxValue[i] = ((long) maxValue[i] > valueL ? maxValue[i] : valueL);
                    minValue[i] = ((double) minValue[i] < valueL ? minValue[i] : valueL);
                    uniqueValue[i] = (double) minValue[i] - 1;
                    num = (valueL % 1 == 0) ? 0 : decimalPointers;

                    break;
                case 'b':

                    BigDecimal valueD = aggregators[i].getBigDecimalValue();
                    maxValue[i] = (new BigDecimal(maxValue[i].toString()).compareTo(valueD) == 1 ?
                            maxValue[i] :
                            valueD);
                    minValue[i] = (new BigDecimal(maxValue[i].toString()).compareTo(valueD) == -1 ?
                            minValue[i] :
                            valueD);
                    uniqueValue[i] = (double) minValue[i] - 1;
                    num = (valueD.doubleValue() % 1 == 0) ? 0 : decimalPointers;
                    break;
                default:

                    double value = aggregators[i].getDoubleValue();
                    maxValue[i] = ((double) maxValue[i] > value ? maxValue[i] : value);
                    minValue[i] = ((double) minValue[i] < value ? minValue[i] : value);
                    uniqueValue[i] = (double) minValue[i] - 1;
                    num = (value % 1 == 0) ? 0 : decimalPointers;
                }

                decimalLength[i] = (decimalLength[i] > num ? decimalLength[i] : num);
            }
        }
    }

    /**
     * below method will be used to finish group by
     *
     * @throws MolapGroupByException
     */
    public void finish() throws MolapGroupByException {

        MolapUtil.closeStreams(readingStream);

        try {
            MolapUtil.deleteFoldersAndFiles(storeLocation);
        } catch (MolapUtilException e) {
            throw new MolapGroupByException(e);
        }
        customAggHelper = null;
    }

}

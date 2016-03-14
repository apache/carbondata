/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/q9KhY20o/YLg052bWU9Ft3c57Y0ku0JJItUIkMXZChXFCOZSDHeantvu7wQZy39HeDI
H2NG1Wg9UMhNgdZyJDZzeft+gknejEUR8RHU4kCI0NrZUdSc2A6T2FbzC/Z4TA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.store.writer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : MolapDataWriter.java
 * Class Description : This class is responsible for writing the molap data and its meta data in file.
 *
 * Version 1.0
 */
public class MolapDataWriter
{
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapDataWriter.class.getName());
    /**
     * tabel name
     */
    private String tableName;
    
    /**
     * data file size;
     */
    private long fileSizeInBytes;

    /**
     * measure count
     */
    private int measureCount;
    
    /**
     * this will be used for holding leaf node metadata
     */
    private List<LeafNodeInfo> leafNodeInfoList;
    
    /**
     * current size of file
     */
    private long currentFileSize;
    
    /**
     * leaf metadata size
     */
    private int leafMetaDataSize;
    
    /**
     * file count will be used to give sequence number to the leaf node file
     */
    private int fileCount;

    /**
     * Leaf node filename format
     */
    private String fileNameFormat;

    /**
     * leaf node file channel
     */
    private FileChannel fileChannel;
    
    /**
     * leaf node file name
     */
    private String fileName;
    
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;

    
    /**
     * Store Location
     */
    private String storeLocation;
    
    /**
     * isNodeHolderRequired
     */
    private boolean isNodeHolderRequired;
    
    /**
     * Node Holder
     */
    private List<NodeHolder> nodeHolderList;
    
    /**
     * executorService
     */
    private ExecutorService executorService;

    /**
     * 
     * MolapDataWriter constructor to initialize all the instance variables
     * required for wrting the data i to the file
     * 
     * @param storeLocation
     *          current store location
     * @param measureCount
     *          total number of measures
     * @param mdKeyLength
     *          mdkey length
     * @param tableName
     *          table name
     * @param fileSizeInBytes
     *          file size in bytes
     * 
     */
    public MolapDataWriter(String storeLocation, int measureCount, int mdKeyLength, String tableName, boolean isNodeHolder)
    {
        // measure count
        this.measureCount = measureCount;
        // table name
        this.tableName=tableName;
        
        this.storeLocation = storeLocation;
        // create the leaf node file format
        this.fileNameFormat = storeLocation + File.separator + this.tableName
                + '_' + "{0}"+ MolapCommonConstants.FACT_FILE_EXT;

        this.fileName = MessageFormat.format(this.fileNameFormat, this.fileCount);
        // leaf meta data size
        // measure length, key length and number of keys will be INT size
        // measure offset and key offset will be in long
        // startkey and end key 
        this.leafMetaDataSize = MolapCommonConstants.INT_SIZE_IN_BYTE * (2 + measureCount)
                + MolapCommonConstants.LONG_SIZE_IN_BYTE * (measureCount + 1) + (2 * mdKeyLength);
        this.leafNodeInfoList = new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        // get max file size;
        MolapProperties instance = MolapProperties.getInstance();
        this.fileSizeInBytes = Long.parseLong(instance.getProperty(MolapCommonConstants.MAX_FILE_SIZE,
                MolapCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL))
                * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                *1L;
      //CHECKSTYLE:OFF    Approval No:Approval-323
        this.isNodeHolderRequired = Boolean
                .valueOf(MolapCommonConstants.WRITE_ALL_NODE_IN_SINGLE_TIME_DEFAULT_VALUE);
      //CHECKSTYLE:ON
        
        this.isNodeHolderRequired = this.isNodeHolderRequired && isNodeHolder;
        if(this.isNodeHolderRequired)
        {
            this.nodeHolderList= new CopyOnWriteArrayList<NodeHolder>();
            
            this.executorService= Executors.newFixedThreadPool(5);
        }
        
    }

    /**
     * This method will be used to initialize the channel
     * @throws MolapDataWriterException 
     */
    public void initChannel() throws MolapDataWriterException 
    {
     // update the filename with new new sequence
        // increment the file sequence counter
        initFileCount();
        this.fileName = MessageFormat.format(this.fileNameFormat,
                this.fileCount);
        String actualFileName = this.tableName + '_' + this.fileCount + MolapCommonConstants.FACT_FILE_EXT
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        FileData fileData = new FileData(actualFileName, this.storeLocation);
        fileManager.add(fileData);
        this.fileName = this.fileName + MolapCommonConstants.FILE_INPROGRESS_STATUS;

        this.fileCount++;
        try
        {
            // open channle for new leaf node file
            this.fileChannel = new FileOutputStream(this.fileName, true).getChannel();
        }
        catch(FileNotFoundException fileNotFoundException)
        {
            throw new MolapDataWriterException("Problem while getting the FileChannel for Leaf File", fileNotFoundException);
        }
    }
    
    /**
     * Method will be used to close the open file channel
     * @throws MolapDataWriterException 
     * 
     *
     */
    public void closeChannle()
    {   
        if(!this.isNodeHolderRequired)
        {
            MolapUtil.closeStreams(this.fileChannel);
            // close channel
        }
        else
        {
            this.executorService.shutdown();
            try
            {
                this.executorService.awaitTermination(2, TimeUnit.HOURS);
            }
            catch(InterruptedException ie) 
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ie);
            }
            MolapUtil.closeStreams(this.fileChannel);
            this.nodeHolderList = null;
        }
        
        File originalMolapFile = new File(this.fileName.substring(0,
                this.fileName.lastIndexOf('.')));
        File currFile = new File(this.fileName); 
        if(!currFile.renameTo(originalMolapFile))
        {
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Problem while renaming the file");
        }
        
        if(originalMolapFile.length()<1)
        { 
            if(!originalMolapFile.delete())
            {
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Problem while deleting the empty fact file");
            }
        }
    }
    
    //TODO SIMIAN
    private int initFileCount()
    {
        int filesCnt = 0;
        File[] dataFiles = new File(storeLocation).listFiles(new FileFilter()
        {

            @Override 
            public boolean accept(File f)
            {
                if(!f.isDirectory() && f.getName().startsWith(tableName) 
                        && f.getName().contains(MolapCommonConstants.FACT_FILE_EXT))
                {
                    return true;
                }
                return false;
            }
        });
        if(dataFiles != null && dataFiles.length > 0)
        {
            Arrays.sort(dataFiles);
            String fileName = dataFiles[dataFiles.length - 1].getName();
            try
            {
                filesCnt = Integer.parseInt(fileName.substring(fileName.lastIndexOf('_') + 1).split("\\.")[0]);
            }
            catch(NumberFormatException ex)
            {
                filesCnt = 0;
            }
            filesCnt++;
        }
        return filesCnt;
    }
    
    /**
     * This method will be used to update the file channel with new file; new
     * file will be created once existing file reached the file size limit This
     * method will first check whether existing file size is exceeded the file
     * size limit if yes then write the leaf metadata to file then set the
     * current file size to 0 close the existing file channel get the new file
     * name and get the channel for new file
     * @throws MolapDataWriterException 
     *              if any problem
     * 
     */
    private void updateLeafNodeFileChannel() throws MolapDataWriterException
    {
        // get the current file size exceeding the file size threshold
        if(currentFileSize>=fileSizeInBytes)
        {
            // set the current file size to zero
            this.currentFileSize = 0;
            if(this.isNodeHolderRequired)
            {
                FileChannel channel = fileChannel;
                List<NodeHolder> localNodeHolderList=this.nodeHolderList;
                executorService.submit(new WriterThread(channel, localNodeHolderList));
                this.nodeHolderList = new CopyOnWriteArrayList<NodeHolder>();
             // close the current open file channel
            }
            else
            {
                // write meta data to end of the existing file
                writeleafMetaDataToFile(this.leafNodeInfoList, fileChannel);
                this.leafNodeInfoList = new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                MolapUtil.closeStreams(fileChannel);
            }
            // initialize the new channel
            initChannel();
        }
    }
    
    /**
     * Write leaf meta data to File.
     * 
     * @throws MolapDataWriterException
     *
     */
    public void writeleafMetaDataToFile() throws MolapDataWriterException
    {
        if(!isNodeHolderRequired)
        {
            writeleafMetaDataToFile(this.leafNodeInfoList, fileChannel);
            this.leafNodeInfoList = new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        }
        else
        {
            if(this.nodeHolderList.size()>0)
            {
               List<NodeHolder> localNodeHodlerList=nodeHolderList;
               writeData(fileChannel, localNodeHodlerList);
               nodeHolderList= new CopyOnWriteArrayList<NodeHolder>();
            }
        }
    }

    /**
     * This method will be used to write leaf data to file
     * 
     * file format
     * <key><measure1><measure2>....
     * @param keyArray
     *          key array
     * @param dataArray
     *          measure array
     * @param entryCount
     *          number of entries
     * @param startKey
     *          start key of leaf
     * @param endKey
     *          end key of leaf
     * @throws MolapDataWriterException 
     * @throws MolapDataWriterException
     *          throws new MolapDataWriterException if any problem
     *
     */
    public void writeDataToFile(byte[] keyArray, byte[][] dataArray, int entryCount, byte[] startKey, byte[] endKey) 
            throws MolapDataWriterException
    {
        updateLeafNodeFileChannel();
        // total measure length;
        int totalMsrArraySize = 0;
        // current measure length;
        int currentMsrLenght = 0;
        int[] msrLength = new int[this.measureCount];

        // calculate the total size required for all the measure and get the
        // each measure size
        for(int i = 0;i < dataArray.length;i++)
        {
            currentMsrLenght = dataArray[i].length;
            totalMsrArraySize += currentMsrLenght;
            msrLength[i] = currentMsrLenght;
        }
        byte[] writableDataArrayTemp = new byte[totalMsrArraySize];

        // start position will be used for adding the measure in
        // writableDataArray after adding measure increment the start position
        // by added measure length which will be used for next measure start
        // position
        int startPosition = 0;
        for(int i = 0;i < dataArray.length;i++)
        {
            System.arraycopy(dataArray[i], 0, writableDataArrayTemp, startPosition, dataArray[i].length);
            startPosition += msrLength[i];
        }
        // current file size;
        this.currentFileSize += keyArray.length + writableDataArrayTemp.length;
        
        if(!this.isNodeHolderRequired)
        {
            writeDataToFile(keyArray, writableDataArrayTemp, msrLength, entryCount, startKey, endKey);
        }
        else
        {
            NodeHolder nodeHolder = new NodeHolder();
            nodeHolder.setDataArray(writableDataArrayTemp);
            nodeHolder.setKeyArray(keyArray);
            nodeHolder.setEndKey(endKey);
            nodeHolder.setMeasureLenght(msrLength);
            nodeHolder.setStartKey(startKey);
            nodeHolder.setEntryCount(entryCount);
            this.nodeHolderList.add(nodeHolder);
        }
    }
    
    /**
     * This method will be used to write leaf data to file
     * 
     * file format
     * <key><measure1><measure2>....
     * @param keyArray
     *          key array
     * @param dataArray
     *          measure array
     * @param entryCount
     *          number of entries
     * @param startKey
     *          start key of leaf
     * @param endKey
     *          end key of leaf
     * @throws MolapDataWriterException 
     * @throws MolapDataWriterException
     *          throws new MolapDataWriterException if any problem
     *
     */
    public void writeDataToFile(byte[] keyArray, byte[] dataArray, int[] msrLength, int entryCount, byte[] startKey,
            byte[] endKey) 
            throws MolapDataWriterException
    {
        int keySize = keyArray.length;
        // write data to leaf file and get its offset
        long offset = writeDataToFile(keyArray, dataArray, this.fileChannel);

        // get the leaf node info for currently added leaf node
        LeafNodeInfo leafNodeInfo = getLeafNodeInfo(keySize, msrLength, offset, entryCount, startKey, endKey);
        // add leaf info to list
        this.leafNodeInfoList.add(leafNodeInfo);
        // calculate the current size of the file
    }
    
    /**
     * This method will be used to get the leaf node metadata 
     * 
     * @param keySize
     *          key size
     * @param msrLength
     *          measure length array
     * @param offset
     *          current offset
     * @param entryCount
     *          total number of rows in leaf 
     * @param startKey
     *          start key of leaf 
     * @param endKey
     *          end key of leaf
     * @return LeafNodeInfo - leaf metadata
     *
     */
    private LeafNodeInfo getLeafNodeInfo(int keySize, int[] msrLength, long offset, int entryCount, byte[] startKey,
            byte[] endKey)
    {
        // create the info object for leaf entry
        LeafNodeInfo nodeInfo = new LeafNodeInfo();
        // add total entry count
        nodeInfo.setNumberOfKeys(entryCount);

        // add the key array length
        nodeInfo.setKeyLength(keySize);

        // key offset will be 8 bytes from current offset because first 4 bytes
        // will be for number of entry in leaf, then next 4 bytes will be for
        // key lenght;
//        offset += MolapCommonConstants.INT_SIZE_IN_BYTE * 2;

        // add key offset
        nodeInfo.setKeyOffset(offset);

        // increment the current offset by adding key length to get the measure
        // offset position
        // format of metadata will be
        // <entrycount>,<keylenght>,<keyoffset>,<msr1lenght><msr1offset><msr2length><msr2offset>
        offset += keySize;

        // add measure length
        nodeInfo.setMeasureLength(msrLength);

        long[] msrOffset = new long[this.measureCount];

        for(int i = 0;i < this.measureCount;i++)
        {
            // increment the current offset by 4 bytes because 4 bytes will be
            // used for measure byte length
//            offset += MolapCommonConstants.INT_SIZE_IN_BYTE;
            msrOffset[i] = offset;
            // now increment the offset by adding measure length to get the next
            // measure offset;
            offset += msrLength[i];
        }
        // add measure offset
        nodeInfo.setMeasureOffset(msrOffset);
        // set startkey
        nodeInfo.setStartKey(startKey);
        // set end key
        nodeInfo.setEndKey(endKey);
        // return leaf metadata
        return nodeInfo;
    }

    /**
     * This method is responsible for writing leaf node to the leaf node file
     * 
     * @param keyArray
     *            mdkey array
     * @param measureArray
     *            measure array
     * @param fileName
     *            leaf node file
     * @return file offset offset is the current position of the file
     * @throws MolapDataWriterException 
     *          if will throw MolapDataWriterException when any thing goes wrong
     *             while while writing the leaf file
     */
    private long writeDataToFile(byte[] keyArray, byte[] measureArray, FileChannel channel) throws MolapDataWriterException
    {
        // create byte buffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(keyArray.length + measureArray.length);
        long offset = 0;
        try
        {
            // get the current offset
            offset = channel.size();
            // add key array to byte buffer
            byteBuffer.put(keyArray);
            // add measure data array to byte buffer
            byteBuffer.put(measureArray);
            byteBuffer.flip();
            // write data to file
            channel.write(byteBuffer);
        }
        catch(IOException exception)
        {
            throw new MolapDataWriterException("Problem in writing Leaf Node File: ", exception);
        }
        // return the offset, this offset will be used while reading the file in
        // engine side to get from which position to start reading the file
        return offset;
    }

    /**
     * This method will write metadata at the end of file file format
     * <KeyArray><measure1><measure2> <KeyArray><measure1><measure2>
     * <KeyArray><measure1><measure2> <KeyArray><measure1><measure2>
     * <entrycount>
     * <keylength><keyoffset><measure1length><measure1offset><measure2length
     * ><measure2offset>
     * 
     * @throws MolapDataWriterException
     *             throw MolapDataWriterException when problem in writing the meta data
     *             to file
     * 
     */
    public void writeleafMetaDataToFile(List<LeafNodeInfo> infoList, FileChannel channel) throws MolapDataWriterException 
    {
        ByteBuffer buffer = null;
        long currentPosition = 0;
        int[] msrLength = null;
        long[] msroffset = null;
        try
        {
            // get the current position of the file, this will be used for
            // reading the file meta data, meta data start position in file will
            // be this position
            currentPosition = channel.size();
            for(LeafNodeInfo info : infoList)
            {
                // get the measure length array
                msrLength = info.getMeasureLength();
                // get the measure offset array
                msroffset = info.getMeasureOffset();
                // allocate total size for buffer
                buffer = ByteBuffer.allocate(leafMetaDataSize);
                // add entry count
                buffer.putInt(info.getNumberOfKeys());
                // add key length
                buffer.putInt(info.getKeyLength());
                // add key offset
                buffer.putLong(info.getKeyOffset());
                // set the start key
                buffer.put(info.getStartKey());
                // set the end key
                buffer.put(info.getEndKey());
                // add each measure length and its offset
                for(int i = 0;i < msrLength.length;i++)
                {
                    buffer.putInt(msrLength[i]);
                    buffer.putLong(msroffset[i]);
                }
                // flip the buffer
                buffer.flip();
                // write metadat to file
                channel.write(buffer);
            }
            // create new for adding the offset of meta data
            buffer = ByteBuffer.allocate(MolapCommonConstants.LONG_SIZE_IN_BYTE);
            // add the offset
            buffer.putLong(currentPosition);
            buffer.flip();
            // write offset to file
            channel.write(buffer);
        }
        catch(IOException exception)
        {
            throw new MolapDataWriterException("Problem while writing the Leaf Node File: ", exception);
        }
    }
    
    /**
     * This method will be used to get the leaf meta list size
     * 
     * @return list size
     *
     */
    public int getMetaListSize()
    {
        return leafNodeInfoList.size();
    }

	/**
     * @param fileManager2
     */
    public void setFileManager(IFileManagerComposite fileManager)
    {
        this.fileManager = fileManager;
    }

    /**
     * getFileCount
     * @return int
     */
    public int getFileCount()
    {
        return fileCount;
    }

    /**
     * setFileCount
     * @param fileCount void
     */
    public void setFileCount(int fileCount)
    {
        this.fileCount = fileCount;
    }
  //CHECKSTYLE:OFF    Approval No:Approval-323
    /**
     * Thread class for writing data to file 
     * @author k00900841
     *
     */
    private final class WriterThread implements Callable<Void>
    {
        private FileChannel channel;
        
        private List<NodeHolder> nodeHolderList;
        
        private WriterThread(FileChannel channel, List<NodeHolder> nodeHolderList)
        {
            this.channel=channel;
            this.nodeHolderList=nodeHolderList;
        }
        @Override
        public Void call() throws Exception
        {
            writeData(channel,nodeHolderList);
            return null;
        }
    }
  //CHECKSTYLE:ON
    /**
     * Below method will be used to write data and its meta data to file 
     * @param channel
     * @param nodeHolderList
     * @throws MolapDataWriterException
     */
    private void writeData(FileChannel channel,List<NodeHolder> nodeHolderList) throws MolapDataWriterException
    {
        List<LeafNodeInfo> leafMetaInfos= new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(NodeHolder nodeHolder:nodeHolderList)
        {
            long offSet = writeDataToFile(nodeHolder.getKeyArray(), nodeHolder.getDataArray(), channel);
            leafMetaInfos.add(getLeafNodeInfo(nodeHolder.getKeyArray().length,
                    nodeHolder.getMeasureLenght(), offSet,
                    nodeHolder.getEntryCount(), nodeHolder.getStartKey(),
                    nodeHolder.getEndKey()));
        }
        writeleafMetaDataToFile(leafMetaInfos, channel);
        MolapUtil.closeStreams(channel);
    }
}

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
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

public abstract class AbstractFactDataWriter<T> implements MolapFactDataWriter<T>

{
	
	private static final LogService LOGGER = LogServiceFactory
            .getLogService(AbstractFactDataWriter.class.getName());
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
    protected int measureCount;
    
    /**
     * current size of file
     */
    protected long currentFileSize;
    
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
    protected FileChannel fileChannel;
    
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
    protected boolean isNodeHolderRequired;
    
    /**
     * Node Holder
     */
    protected List<NodeHolder> nodeHolderList;
    
    /**
     * executorService
     */
    private ExecutorService executorService;
    
    /**
     * this will be used for holding leaf node metadata
     */
    protected List<LeafNodeInfoColumnar> leafNodeInfoList;
    
    /**
     * keyBlockSize
     */
    protected int[] keyBlockSize;
    
    protected boolean[] isNoDictionary;
    
    /**
     * @param isNoDictionary the isNoDictionary to set
     */
    public void setIsNoDictionary(boolean[] isNoDictionary)
    {
        this.isNoDictionary = isNoDictionary;
    }

    /**
     * mdkeySize
     */
    protected int mdkeySize;
    
	public AbstractFactDataWriter(String storeLocation, int measureCount,
			int mdKeyLength, String tableName, boolean isNodeHolder,
			IFileManagerComposite fileManager, int[] keyBlockSize, boolean isUpdateFact) 
	{
	 	  
		// measure count
        this.measureCount = measureCount;
        // table name
        this.tableName=tableName;
        
        this.storeLocation = storeLocation;
        // create the leaf node file format
        if(isUpdateFact)
        {
        	   this.fileNameFormat = System.getProperty("java.io.tmpdir") + File.separator + this.tableName
                       + '_' + "{0}"+ MolapCommonConstants.FACT_UPDATE_EXTENSION;
        }
        else
        {
        this.fileNameFormat = storeLocation + File.separator + this.tableName
                + '_' + "{0}"+ MolapCommonConstants.FACT_FILE_EXT;
        }

        this.fileName = MessageFormat.format(this.fileNameFormat, this.fileCount);
        this.leafNodeInfoList = new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        // get max file size;
        MolapProperties propInstance = MolapProperties.getInstance();
        this.fileSizeInBytes = Long.parseLong(propInstance.getProperty(MolapCommonConstants.MAX_FILE_SIZE,
                MolapCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL))
                * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                *1L;
      //CHECKSTYLE:OFF    Approval No:Approval-323
        this.isNodeHolderRequired = Boolean
                .valueOf(MolapCommonConstants.WRITE_ALL_NODE_IN_SINGLE_TIME_DEFAULT_VALUE);
        this.fileManager=fileManager;	
    
        /**
         * keyBlockSize
         */
        this.keyBlockSize=keyBlockSize;
        /**
         * 
         */
        this.mdkeySize=mdKeyLength;
      //CHECKSTYLE:ON
        
        this.isNodeHolderRequired = this.isNodeHolderRequired && isNodeHolder;
        if(this.isNodeHolderRequired)
        {
            this.nodeHolderList= new CopyOnWriteArrayList<NodeHolder>();
            
            this.executorService= Executors.newFixedThreadPool(5);
        }
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
    protected void updateLeafNodeFileChannel() throws MolapDataWriterException
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
                writeleafMetaDataToFile(leafNodeInfoList, fileChannel);
                leafNodeInfoList = new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                MolapUtil.closeStreams(fileChannel);
            }
            // initialize the new channel
            initializeWriter();
        }
    }
    
	/**
     * This method will be used to initialize the channel
     * @throws MolapDataWriterException 
     */
    public void initializeWriter() throws MolapDataWriterException 
    {
     // update the filename with new new sequence
        // increment the file sequence counter
        initFileCount();
        this.fileName = MessageFormat.format(this.fileNameFormat,
                this.fileCount);
        String actualFileNameVal = this.tableName + '_' + this.fileCount + MolapCommonConstants.FACT_FILE_EXT
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        FileData fileData = new FileData(actualFileNameVal, this.storeLocation);
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
    
    private int initFileCount()
    {
        int fileInitialCount = 0;
        File[] dataFiles = new File(storeLocation).listFiles(new FileFilter()
        {

            @Override
            public boolean accept(File pathVal)
            {
                if(!pathVal.isDirectory() && pathVal.getName().startsWith(tableName)
                        && pathVal.getName().contains(MolapCommonConstants.FACT_FILE_EXT))
                {
                    return true;
                }
                return false;
            }
        });
        if(dataFiles != null && dataFiles.length > 0)
        {
            Arrays.sort(dataFiles); 
            String dataFileName = dataFiles[dataFiles.length - 1].getName();
            try
            {
                fileInitialCount = Integer.parseInt(dataFileName.substring(dataFileName.lastIndexOf('_') + 1).split("\\.")[0]);
            }
            catch(NumberFormatException ex)
            {
                fileInitialCount = 0;
            }
            fileInitialCount++;
        }
        return fileInitialCount;
    }
    
    //TODO SIMIAN
    /**
     * Thread class for writing data to file 
     * @author k00900841
     *
     */
    private final class WriterThread implements Callable<Void>
    {
        
        private List<NodeHolder> nodeHolderList;
        
        private FileChannel channel;

        
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
        List<LeafNodeInfoColumnar> leafMetaInfos= new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(NodeHolder nodeHolder:nodeHolderList)
        {
            long offSet = writeDataToFile(nodeHolder, channel);
            leafMetaInfos.add(getLeafNodeInfo(nodeHolder, offSet));
        }
        writeleafMetaDataToFile(leafMetaInfos, channel);
        MolapUtil.closeStreams(channel);
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
    protected void writeleafMetaDataToFile(List<LeafNodeInfoColumnar> infoList, FileChannel channel) throws MolapDataWriterException 
    {
        ByteBuffer buffer = null;
        long currentPosition = 0;
        int[] msrLength = null;
        long[] msroffset = null;
        int[] keyLengths = null;
        long[] keyOffSets= null;
        //column min max data
        byte[][] columnMinMaxData=null;
        
        int[] keyBlockIndexLengths = null;
        long[] keyBlockIndexOffSets= null;
        boolean[] isSortedKeyColumn = null;
        
        try
        {
            // get the current position of the file, this will be used for
            // reading the file meta data, meta data start position in file will
            // be this position
            currentPosition = channel.size();
            for(LeafNodeInfoColumnar info : infoList)
            {
                // get the measure length array
                msrLength = info.getMeasureLength();
                // get the measure offset array
                msroffset = info.getMeasureOffset();
                //get the key length
                keyLengths= info.getKeyLengths();
                // get the key offsets
                keyOffSets= info.getKeyOffSets();
                //keyBlockIndexLengths
                keyBlockIndexLengths= info.getKeyBlockIndexLength();
                //keyOffSets
                keyBlockIndexOffSets=info.getKeyBlockIndexOffSets();
                //isSortedKeyColumn
                isSortedKeyColumn=info.getIsSortedKeyColumn();
                // allocate total size for buffer
                buffer = ByteBuffer.allocate(info.getLeafNodeMetaSize());
                // add entry count
                buffer.putInt(info.getNumberOfKeys());
                buffer.putInt(keyOffSets.length);
                for (int i = 0; i < keyOffSets.length; i++)
                {
                	 // add key length
                    buffer.putInt(keyLengths[i]);
                    // add key offset
                    buffer.putLong(keyOffSets[i]);
                    buffer.put(isSortedKeyColumn[i]?(byte)0:(byte)1);
				}
                
                //set column min max data
                columnMinMaxData=info.getColumnMinMaxData();
                buffer.putInt(columnMinMaxData.length);
                for(int j=0;j<columnMinMaxData.length;j++)
                {
                	buffer.putInt(columnMinMaxData[j].length);
                	buffer.put(columnMinMaxData[j]);
                }
                
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
                buffer.putInt(keyBlockIndexLengths.length);
                for(int i = 0;i < keyBlockIndexLengths.length;i++)
                {
                	buffer.putInt(keyBlockIndexLengths[i]);
                    buffer.putLong(keyBlockIndexOffSets[i]);
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
    
    protected int calculateAndSetLeafNodeMetaSize(NodeHolder nodeHolder)
    {
    	int metaSize=0;
    	//measure offset and measure length
    	metaSize+= (measureCount * MolapCommonConstants.INT_SIZE_IN_BYTE)
				+ (measureCount * MolapCommonConstants.LONG_SIZE_IN_BYTE);
    	//start and end key 
    	metaSize+=mdkeySize*2;
    	
    	// keyblock length + key offsets + number of tuples+ number of columnar block
    	metaSize+=(nodeHolder.getKeyLengths().length * MolapCommonConstants.INT_SIZE_IN_BYTE)
				+ (nodeHolder.getKeyLengths().length * MolapCommonConstants.LONG_SIZE_IN_BYTE)+MolapCommonConstants.INT_SIZE_IN_BYTE
				+ MolapCommonConstants.INT_SIZE_IN_BYTE;
    	//if sorted or not 
    	metaSize+=nodeHolder.getIsSortedKeyBlock().length;
    	
    	//column min max size
        //for length of columnMinMax byte array
        metaSize+=MolapCommonConstants.INT_SIZE_IN_BYTE;
        for(int i=0;i<nodeHolder.getColumnMinMaxData().length;i++)
        {
        	//length of sub byte array
        	metaSize+=MolapCommonConstants.INT_SIZE_IN_BYTE;
        	metaSize+=nodeHolder.getColumnMinMaxData()[i].length;
        }
    	
    	// key block index length + key block index offset + number of key block
    	metaSize+= (nodeHolder
				.getKeyBlockIndexLength().length * MolapCommonConstants.INT_SIZE_IN_BYTE)
				+ (nodeHolder.getKeyBlockIndexLength().length * MolapCommonConstants.LONG_SIZE_IN_BYTE)+MolapCommonConstants.INT_SIZE_IN_BYTE;
		return metaSize;
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
    protected LeafNodeInfoColumnar getLeafNodeInfo(NodeHolder nodeHolder, long offset)
    {
        // create the info object for leaf entry
    	LeafNodeInfoColumnar infoObj = new LeafNodeInfoColumnar(); 
        // add total entry count
        infoObj.setNumberOfKeys(nodeHolder.getEntryCount());

        // add the key array length
        infoObj.setKeyLengths(nodeHolder.getKeyLengths());
        //add column min max data
        infoObj.setColumnMinMaxData(nodeHolder.getColumnMinMaxData());
        
        long[] keyOffSets= new long[nodeHolder.getKeyLengths().length];
        
        for (int i = 0; i < keyOffSets.length; i++) 
        {
        	keyOffSets[i]=offset;
        	offset+=nodeHolder.getKeyLengths()[i];
		}
        // key offset will be 8 bytes from current offset because first 4 bytes
        // will be for number of entry in leaf, then next 4 bytes will be for
        // key lenght;
//        offset += MolapCommonConstants.INT_SIZE_IN_BYTE * 2;

        // add key offset
        infoObj.setKeyOffSets(keyOffSets);

        // add measure length
        infoObj.setMeasureLength(nodeHolder.getMeasureLenght());

        long[] msrOffset = new long[this.measureCount];

        for(int i = 0;i < this.measureCount;i++)
        {
            // increment the current offset by 4 bytes because 4 bytes will be
            // used for measure byte length
//            offset += MolapCommonConstants.INT_SIZE_IN_BYTE;
            msrOffset[i] = offset;
            // now increment the offset by adding measure length to get the next
            // measure offset;
            offset += nodeHolder.getMeasureLenght()[i];
        }
        // add measure offset
        infoObj.setMeasureOffset(msrOffset);
        infoObj.setIsSortedKeyColumn(nodeHolder.getIsSortedKeyBlock());
        infoObj.setKeyBlockIndexLength(nodeHolder.getKeyBlockIndexLength());
        long[] keyBlockIndexOffsets= new long[nodeHolder.getKeyBlockIndexLength().length];
        for (int i = 0; i < keyBlockIndexOffsets.length; i++) 
        {
        	keyBlockIndexOffsets[i]=offset;
        	offset+=nodeHolder.getKeyBlockIndexLength()[i];
		}
        infoObj.setKeyBlockIndexOffSets(keyBlockIndexOffsets);
        // set startkey
        infoObj.setStartKey(nodeHolder.getStartKey());
        // set end key
        infoObj.setEndKey(nodeHolder.getEndKey());
        infoObj.setLeafNodeMetaSize(calculateAndSetLeafNodeMetaSize(nodeHolder));
        // return leaf metadata
        return infoObj;
    }
    
    /**
     * Method will be used to close the open file channel
     * @throws MolapDataWriterException 
     * 
     *
     */
    public void closeWriter()
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
            catch(InterruptedException ex)
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
            }
            MolapUtil.closeStreams(this.fileChannel);
            this.nodeHolderList = null;
        }
        
        File origFile = new File(this.fileName.substring(0,
                this.fileName.lastIndexOf('.')));
        File curFile = new File(this.fileName);
        if(!curFile.renameTo(origFile))
        {
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Problem while renaming the file");
        }
        if(origFile.length()<1)
        {
            if(!origFile.delete())
            {
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Problem while deleting the empty fact file");
            }
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
            this.leafNodeInfoList = new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
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
    protected void writeDataToFile(NodeHolder nodeHolder) 
            throws MolapDataWriterException
    {
        // write data to leaf file and get its offset
        long offset = writeDataToFile(nodeHolder, fileChannel);

        // get the leaf node info for currently added leaf node
        LeafNodeInfoColumnar leafNodeInfo = getLeafNodeInfo(nodeHolder, offset);
        // add leaf info to list
        leafNodeInfoList.add(leafNodeInfo);
        // calculate the current size of the file
    }
    
    protected abstract long writeDataToFile(NodeHolder nodeHolder,FileChannel channel) throws MolapDataWriterException;
    
    @Override
	public int getLeafMetadataSize() 
    {
    	return leafNodeInfoList.size();
		
	}
    
	@Override
	public String getTempStoreLocation() {
		
		return this.fileName;
	}

}

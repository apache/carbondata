/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XaTLWuR1R9rC8F8A6PutEFhXTNkbSlgbc6Z0sw95g2Eak3r1x3QL0453AfG6SPTi2pAij
zJCGw+Gdd6tfXB3hyrAAumg+j4ChVFcbZT0XIpIlDbfUVg3KhkVz3b3BxdZpRQ==*/
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
package com.huawei.unibi.molap.merger.sliceMerger;



import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.merger.Util.RowTempFile;
import com.huawei.unibi.molap.merger.exeception.SliceMergerException;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : HierarchyMergerExecuter.java
 * Class Description : This class is responsible for merging the Hierarchy files 
 * Version 1.0
 */
public class HierarchyMergerExecuter
{
    /**
     * file map
     */
    private Map<String, List<MolapFile>> filesMap;

    /**
     * map which will have Hierarchy and its key size 
     */
    private Map<String, Integer> hierarchyAndKeySizeMap;

    /**
     * destination location
     */
    private String destinationLocation;
    
    /**
     * destination file output stream
     */
    private OutputStream destinationFileOutputStream;

    /**
     * record holder heap
     */
    private AbstractQueue<RowTempFile> recordHolderHeap;

    /**
     * HierarchyMergerExecuter constructor 
     * 
     * @param filesMap
     *          file map 
     * @param hierarchyAndKeySizeMap
     *          hierarchy AndKey Size Map
     * @param desitinationLocation
     *          destination location
     * @param numberOfCores
     *          number of cores
     *
     */
    public HierarchyMergerExecuter(Map<String, List<MolapFile>> filesMap, Map<String, Integer> hierarchyAndKeySizeMap,
            String desitinationLocation)
    {
        this.filesMap = filesMap;
        this.hierarchyAndKeySizeMap = hierarchyAndKeySizeMap;
        this.destinationLocation = desitinationLocation;
    }

    /**
     * Executer method to merge hierarchy files
     * @throws SliceMergerException 
     * 
     *
     */
    public void mergeExecuter() throws SliceMergerException 
    {
        // file name
        String fileName = null;
        // file list
        List<MolapFile> fileList= null;
        // record size;
        int recordSize= 0;
        for(Entry<String, List<MolapFile>> entry : this.filesMap.entrySet())
        {
            // get the file name 
            fileName = entry.getKey();
            // file list
            fileList = entry.getValue();
            // get te record size
            recordSize = hierarchyAndKeySizeMap.get(fileName);
            mergeFiles(fileList, recordSize);
        }
    }
    
    /**
     * Overridden method, this method will be used to merge the files 
     * @throws SliceMergerException 
     * 
     * @see java.util.concurrent.Callable#call()
     *
     */
    public void mergeFiles(List<MolapFile> files, int recordSize) throws SliceMergerException
    {
        int numberOfFiles = files.size();
        createRecordHolderQueue(numberOfFiles,recordSize);
        recordSize=recordSize+MolapCommonConstants.INT_SIZE_IN_BYTE;
        String fileName= destinationLocation
                + File.separator + files.get(0).getName();
        fileName = fileName + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        // create the output stream
        try
        {
            
            this.destinationFileOutputStream = new BufferedOutputStream(new FileOutputStream(fileName));
        }
        catch(FileNotFoundException e)
        {
            throw new SliceMergerException("Problem while finding the Hierarchy File: "+ fileName,  e);
        }
        // get the number of file
        // create the file holder array which will be used to read the files
        FileHolder[] fileHolder = new FileHolder[numberOfFiles];
        int index = 0;
        long currentOffset=0;
        // iterate over the files and pick first record form file and add it in heap
        for(MolapFile file : files)
        {
            fileHolder[index] = FileFactory.getFileHolder(FileFactory.getFileType());
            // add record to heap
            recordHolderHeap.add(new RowTempFile(fileHolder[index].readByteArray(file.getAbsolutePath(), 0,
                    recordSize), fileHolder[index].getFileSize(file.getAbsolutePath()), 0L, index, file
                    .getAbsolutePath()));
            index++;
        }
        try
        {
            while(numberOfFiles > 0)
            {
                // poll the first record from heap
                RowTempFile poll = recordHolderHeap.poll();
                // write record to file

                this.destinationFileOutputStream.write(poll.getRow());

                // increment the offset
                currentOffset = poll.getOffset() + recordSize;
                // if offset if more than file size then eof
                if(currentOffset >= poll.getFileSize())
                {
                    // close the file stream
                    fileHolder[poll.getFileHolderIndex()].finish();
                    // decrement the file counter
                    numberOfFiles--;

                    continue;
                }
                // set the new offset
                poll.setOffset(currentOffset);
                // read the next row and add to heap
                poll.setRow(fileHolder[poll.getFileHolderIndex()].readByteArray(poll.getFilePath(), currentOffset, recordSize));
                recordHolderHeap.add(poll);
            }
        }
        catch(IOException e)
        {
//            System.out.println(e);
            throw new SliceMergerException("Problem while writing the hierarchy File: " + fileName, e);
            
        }
        finally
        {
            for(int i = 0;i < fileHolder.length;i++)
            {
                fileHolder[i].finish();
            }
            MolapUtil.closeStreams(this.destinationFileOutputStream);
        }
        // close the output stream
        MolapUtil.closeStreams(this.destinationFileOutputStream);
        
        //Rename fileName from In-progress to normal files.
        String currectFileName = fileName;
        String destFileName = fileName.substring(0,
                fileName.lastIndexOf('.'));
        File currentFile = new File(currectFileName);
        File destHierFile = new File(destFileName);

        currentFile.renameTo(destHierFile);
    }
    
    private final class RowTempFileComparator implements Comparator<RowTempFile>
    {
        private int compareSize;
        private RowTempFileComparator(int compareSize)
        {
            this.compareSize=compareSize;
        }
        public int compare(RowTempFile r1, RowTempFile r2)
        {
            byte[] b1 = r1.getRow();
            byte[] b2 = r2.getRow();
            int cmp = 0;
            for(int i = 0;i < compareSize;i++)
            {
                int a = (b1[i] & 0xff);
                int b = (b2[i] & 0xff);
                cmp = a - b;
                if(cmp != 0)
                {
                    cmp = cmp < 0 ? -1 : 1;
                    break;
                }
            }
            return cmp;
        }
    }
    
    /**
     * This method will be used to create the record holder heap 
     * 
     *
     */
    private void createRecordHolderQueue(int size, int compareSize)
    {
        recordHolderHeap = new PriorityQueue<RowTempFile>(size,new RowTempFileComparator(compareSize));
    }
}

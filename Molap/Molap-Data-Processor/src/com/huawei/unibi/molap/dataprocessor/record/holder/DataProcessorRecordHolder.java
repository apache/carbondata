/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBN/0Bci9HBGl0yxXEE8xPJqVNeus38w6OE3/KAMLngAumYzwgMc2btvRFFkU1bRhsDXM+
Yr6UHlcMVQ8iLqOcktslr3jhxgLxIVjze6o0NdfJmaDhA+arVAsVcQ1bP/R/WQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.dataprocessor.record.holder;

/**
* Data processor for the record.
*/
public class DataProcessorRecordHolder
{
    
//    private Object[] originalRecords;
    
    private Object[][] processedRecords;
    
    private Object[][] originalRecords;
    
    private int seqNumber;
    
    private int counter;
    
    private int processCounter; 

	/**
	*
	*/
    public DataProcessorRecordHolder(int size, int seqNumber)
    {
        this.originalRecords = new Object[size][];
        this.processedRecords = new Object[size][];
        this.seqNumber = seqNumber;
    }
    
    /**
	*
	*/
    public Object[][] getOriginalRow()
    {
        return originalRecords;
    }

	/**
	*
	*/
    public void addRow(Object[] oriRow)
    {
        originalRecords[counter++] = oriRow;
    }

    /**
	*
	*/
    public void addProcessedRows(Object[] processedRows)
    {
        processedRecords[processCounter++] = processedRows;
    }

	/**
	* Returns the sequence number.
	*/
    public int getSeqNumber()
    {
        return seqNumber;
    }


	/**
	*
	*/
    public Object[][] getProcessedRow()
    {
        return processedRecords;
    }
}

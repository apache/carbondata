package org.carbondata.query.carbon.result.impl;

import java.math.BigDecimal;

import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Result provide class for non filter query 
 * In case of no filter query we need to return
 * complete data  
 *
 */
public class NonFilterQueryScannedResult extends AbstractScannedResult {

	public NonFilterQueryScannedResult(
			BlockExecutionInfo blockExecutionInfo) {
		super(blockExecutionInfo);
	}

	/**
	 * @return dictionary key array for all the dictionary dimension selected in
	 *         query
	 * 
	 */
	@Override
	public byte[] getDictionaryKeyArray() {
		++currentRow;
		return getDictionaryKeyArray(currentRow);
	}

	/**
	 * Below method will be used to get the complex type key array
	 * 
	 * @return complex type key array
	 */
	@Override
	public byte[][] getComplexTypeKeyArray() {
		return getComplexTypeKeyArray(currentRow);
	}

	/**
	 * Below method will be used to get the no dictionary key array for all the
	 * no dictionary dimension selected in query
	 * 
	 * @return no dictionary key array for all the no dictionary dimension
	 */
	@Override
	public byte[][] getNoDictionaryKeyArray() {
		return getNoDictionaryKeyArray(currentRow);
	}

	/**
	 * will return the current valid row id
	 * 
	 * @return valid row id
	 */
	@Override
	public int getCurrenrRowId() {
		return currentRow;
	}

	/**
	 * Return the dimension data based on dimension ordinal
	 * 
	 * @param dimensionOrdinal
	 *            dimension ordinal
	 * @return dimension data
	 */
	@Override
	public byte[] getDimensionKey(int dimensionOrdinal) {
		return getDimensionData(dimensionOrdinal, currentRow);
	}

	/**
	 * Below method will be used to to check whether measure value is null or
	 * for a measure
	 * 
	 * @param ordinal
	 *            measure ordinal
	 * @return is null or not
	 */
	@Override
	public boolean isNullMeasureValue(int ordinal) {
		return isNullMeasureValue(ordinal, currentRow);
	}

	/**
	 * Below method will be used to get the measure value for measure of long
	 * data type
	 * 
	 * @param ordinal
	 *            measure ordinal
	 * @return long value of measure
	 */
	@Override
	public long getLongMeasureValue(int ordinal) {
		return getLongMeasureValue(ordinal, currentRow);
	}

	/**
	 * Below method will be used to get the value of measure of double type
	 * 
	 * @param ordinal
	 *            measure ordinal
	 * @return measure value
	 */
	@Override
	public double getDoubleMeasureValue(int ordinal) {
		return getDoubleMeasureValue(ordinal, currentRow);
	}

	/**
	 * Below method will be used to get the data of big decimal type of a
	 * measure
	 * 
	 * @param ordinal
	 *            measure ordinal
	 * @return measure value
	 */
	@Override
	public BigDecimal getBigDecimalMeasureValue(int ordinal) {
		return getBigDecimalMeasureValue(ordinal, currentRow);
	}

}

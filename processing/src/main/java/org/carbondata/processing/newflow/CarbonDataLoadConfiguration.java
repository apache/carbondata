package org.carbondata.processing.newflow;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;

import org.apache.hadoop.mapred.RecordReader;

public class CarbonDataLoadConfiguration {

  private DataField[] dataFields;

  private String tableName;

  private String storeLocation;

  private String tempStoreLocation;

  private String blockletSize;

  private String batchSize;

  private String sortSize;

  private String[] header;

  private AbsoluteTableIdentifier tableIdentifier;

  private RecordReader<Void, CarbonArrayWritable> recordReader;

  private char[] complexDelimiters;

  public DataField[] getDataFields() {
    return dataFields;
  }

  public void setDataFields(DataField[] dataFields) {
    this.dataFields = dataFields;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getStoreLocation() {
    return storeLocation;
  }

  public void setStoreLocation(String storeLocation) {
    this.storeLocation = storeLocation;
  }

  public String getTempStoreLocation() {
    return tempStoreLocation;
  }

  public void setTempStoreLocation(String tempStoreLocation) {
    this.tempStoreLocation = tempStoreLocation;
  }

  public String getBlockletSize() {
    return blockletSize;
  }

  public void setBlockletSize(String blockletSize) {
    this.blockletSize = blockletSize;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  public String getSortSize() {
    return sortSize;
  }

  public void setSortSize(String sortSize) {
    this.sortSize = sortSize;
  }

  public RecordReader<Void, CarbonArrayWritable> getRecordReader() {
    return recordReader;
  }

  public void setRecordReader(RecordReader<Void, CarbonArrayWritable> recordReader) {
    this.recordReader = recordReader;
  }

  public String[] getHeader() {
    return header;
  }

  public void setHeader(String[] header) {
    this.header = header;
  }

  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(AbsoluteTableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public char[] getComplexDelimiters() {
    return complexDelimiters;
  }

  public void setComplexDelimiters(char[] complexDelimiters) {
    this.complexDelimiters = complexDelimiters;
  }
}

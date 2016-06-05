package org.carbondata.processing.newflow;

import org.carbondata.core.carbon.metadata.datatype.DataType;

public class DataField {

  private String name;

  private DataType dataType;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }
}

package org.carbondata.processing.newflow.complexobjects;

public class StructObject {

  private Object[] data;

  public StructObject(Object[] data) {
    this.data = data;
  }

  public Object[] getData() {
    return data;
  }

  public void setData(Object[] data) {
    this.data = data;
  }

}

package org.apache.carbondata.processing.newflow.complexobjects;

public class ArrayObject {

  private Object[] data;

  public ArrayObject(Object[] data) {
    this.data = data;
  }

  public Object[] getData() {
    return data;
  }

  public void setData(Object[] data) {
    this.data = data;
  }
}

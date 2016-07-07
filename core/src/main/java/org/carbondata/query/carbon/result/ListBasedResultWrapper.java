package org.carbondata.query.carbon.result;

import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

public class ListBasedResultWrapper {

  private ByteArrayWrapper key;

  private Object[] value;

  /**
   * @return the key
   */
  public ByteArrayWrapper getKey() {
    return key;
  }

  /**
   * @param key the key to set
   */
  public void setKey(ByteArrayWrapper key) {
    this.key = key;
  }

  /**
   * @return the value
   */
  public Object[] getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(Object[] value) {
    this.value = value;
  }
}

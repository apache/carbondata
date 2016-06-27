package org.carbondata.core.carbon;

import java.io.Serializable;
import java.util.Map;

import org.carbondata.core.carbon.metadata.datatype.DataType;

/**
 * Column unique identifier
 */
public class ColumnIdentifier implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * column id
   */
  private String columnId;

  /**
   * column properties
   */
  private Map<String, String> columnProperties;

  private DataType dataType;

  /**
   * @param columnId
   * @param columnProperties
   */
  public ColumnIdentifier(String columnId, Map<String, String> columnProperties,
      DataType dataType) {
    this.columnId = columnId;
    this.columnProperties = columnProperties;
    this.dataType = dataType;
  }

  /**
   * @return columnId
   */
  public String getColumnId() {
    return columnId;
  }

  /**
   * @param columnProperty
   * @return
   */
  public String getColumnProperty(String columnProperty) {
    if (null != columnProperties) {
      return columnProperties.get(columnProperty);
    }
    return null;
  }

  public DataType getDataType() {
    return this.dataType;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnId == null) ? 0 : columnId.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ColumnIdentifier other = (ColumnIdentifier) obj;
    if (columnId == null) {
      if (other.columnId != null) {
        return false;
      }
    } else if (!columnId.equals(other.columnId)) {
      return false;
    }
    return true;
  }

}

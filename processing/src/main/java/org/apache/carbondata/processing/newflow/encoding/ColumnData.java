package org.apache.carbondata.processing.newflow.encoding;

public interface ColumnData<E> {

  void setColumnData(E input);

  E getColumnData();

}

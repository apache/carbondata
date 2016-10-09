package org.apache.carbondata.processing.newflow.encoding;

public interface FieldEncoder<E> {

  E encode(Object[] data);

}

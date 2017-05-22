package org.apache.carbondata.core.datastore.impl.array;

/**
 * Created by root1 on 21/5/17.
 */
public abstract class IndexFieldTypes {

  public abstract FieldType getFieldType();

  public abstract int getFieldLength();

  public enum FieldType {
    KEY,
    FIXED,
    VARIABLE;
  }

  public static class KeyFieldType extends IndexFieldTypes {

    @Override public FieldType getFieldType() {
      return FieldType.KEY;
    }

    @Override public int getFieldLength() {
      return -1;
    }
  }

  public static class FixedFieldType extends IndexFieldTypes {

    private short length;

    public FixedFieldType(short length) {
      this.length = length;
    }

    @Override public FieldType getFieldType() {
      return FieldType.KEY;
    }

    @Override public int getFieldLength() {
      return length;
    }
  }

  public static class VariableFieldType extends IndexFieldTypes {

    @Override public FieldType getFieldType() {
      return FieldType.VARIABLE;
    }

    @Override public int getFieldLength() {
      return -1;
    }
  }

}



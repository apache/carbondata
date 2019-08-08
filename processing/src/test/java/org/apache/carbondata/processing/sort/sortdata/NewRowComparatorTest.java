package org.apache.carbondata.processing.sort.sortdata;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import org.junit.Assert;
import org.junit.Test;

public class NewRowComparatorTest {
  private NewRowComparator new_comparator;

  @Test public void checkmap() {
    DataType noDicDataTypes[] =
        { DataTypes.INT, DataTypes.SHORT, DataTypes.BOOLEAN, DataTypes.BYTE, DataTypes.LONG };
    boolean noDicSortColumnMapping[] = { true, true, true, true, true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Field privateField = null;
    try {
      privateField = new_comparator.getClass().getDeclaredField("comparator_map");
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
    privateField.setAccessible(true);
    Map<DataType, SerializableComparator> testmap = new HashMap<>();
    try {
      Map<DataType, SerializableComparator> refMap =
          (Map<DataType, SerializableComparator>) privateField.get(new_comparator);
      Assert.assertTrue(refMap.size() == noDicDataTypes.length);
    } catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  @Test public void compareint() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Integer arr[] = { 1, 7, 5 };
    Integer arr1[] = { 2, 4, 6 };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void compareintreverse() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Integer arr[] = { 1, 2, 3 };
    Integer arr1[] = { 4, 5, 6 };
    int res = new_comparator.compare(arr1, arr);
    Assert.assertTrue(res > 0);
  }

  @Test public void compareshort() {
    DataType noDicDataTypes[] = { DataTypes.SHORT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Short arr[] = { 1, 2, 3 };
    Short arr1[] = { 4, 5, 6 };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparelong() {
    DataType noDicDataTypes[] = { DataTypes.LONG };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Long arr[] = { 1L, 2L, 3L };
    Long arr1[] = { 4L, 5L, 6L };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparefloat() {
    DataType noDicDataTypes[] = { DataTypes.FLOAT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Float arr[] = { 1F, 2F, 3F };
    Float arr1[] = { 4F, 5F, 6F };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void compareboolean() {
    DataType noDicDataTypes[] = { DataTypes.BOOLEAN };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Boolean arr[] = { false };
    Boolean arr1[] = { true };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparebyte() {
    DataType noDicDataTypes[] = { DataTypes.BYTE };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Byte arr[] = { 1, 2, 3 };
    Byte arr1[] = { 4, 5, 6 }
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }
 
  @Test public void comparemixed() {
    DataType noDicDataTypes[] = { DataTypes.INT, DataTypes.FLOAT, DataTypes.BOOLEAN };
    boolean noDicSortColumnMapping[] = { true, true, true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Object arr[] = { 1, 2F, false };
    Object arr1[] = { 4, 5F, true };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }
}

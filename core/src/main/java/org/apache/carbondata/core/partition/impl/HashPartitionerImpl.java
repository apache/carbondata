package org.apache.carbondata.core.partition.impl;

import java.util.Arrays;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.partition.Partitioner;

/**
 * Hash partitioner implementation
 */
public class HashPartitionerImpl implements Partitioner<Object[]> {

  private int numberOfBuckets;

  private Hash[] hashes;

  public HashPartitionerImpl(int[] indexes, DataType[] dataTypes, int numberOfBuckets) {
    this.numberOfBuckets = numberOfBuckets;
    hashes = new Hash[indexes.length];
    for (int i = 0; i < indexes.length; i++) {
      if (dataTypes[indexes[i]] == DataType.STRING) {
        hashes[i] = new ByteArrayHash(indexes[i]);
      } else {
        hashes[i] = new NumericHash(indexes[i]);
      }
    }
  }

  @Override public int getPartition(Object[] objects) {
    int hashCode = 0;
    for (Hash hash : hashes) {
      hashCode += hash.getHash(objects);
    }
    return hashCode % numberOfBuckets;
  }

  private interface Hash {
    int getHash(Object[] value);
  }

  private static class NumericHash implements Hash {

    private int index;

    private NumericHash(int index) {
      this.index = index;
    }

    public int getHash(Object[] value) {
      return (Integer) value[index];
    }
  }

  private static class ByteArrayHash implements Hash {

    private int index;

    private ByteArrayHash(int index) {
      this.index = index;
    }

    @Override public int getHash(Object[] value) {
      return Arrays.hashCode((byte[]) value[index]);
    }
  }
}

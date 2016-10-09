package org.apache.carbondata.hadoop.csvinput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * Created by root1 on 16/4/16.
 */
public class CustomArrayWritable implements Writable {
  private String[] values;

  public String[] toStrings() {
    return values;
  }

  public void set(String[] values) {
    this.values = values;
  }

  public String[] get() {
    return values;
  }

  @Override public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    values = new String[length];
    for (int i = 0; i < length; i++) {
      byte[] b = new byte[in.readInt()];
      in.readFully(b);
      values[i] = new String(b, Charset.defaultCharset());
    }
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
      byte[] b = values[i].getBytes(Charset.defaultCharset());
      out.writeInt(b.length);
      out.write(b);
    }
  }

  @Override public String toString() {
    return Arrays.toString(values);
  }
}

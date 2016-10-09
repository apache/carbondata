//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.carbondata.hadoop.csvinput;

import java.io.IOException;
import java.io.Reader;

public final class CustomReader extends Reader {

  private long remaining;
  private Reader reader;
  private static final byte LF = '\n';
  private boolean lfFound = false;

  public CustomReader(Reader var1) {
    this.reader = var1;
  }

  public int read() throws IOException {
    if (this.remaining == 0) {
      return -1;
    } else {
      int var1 = this.reader.read();
      if (var1 >= 0) {
        --this.remaining;
      }

      return var1;
    }
  }

  public int read(char[] var1, int var2, int var3) throws IOException {
    if (this.remaining == 0) {
      return -1;
    } else {
      if (this.remaining < var3) {
        var3 = (int) this.remaining;
      }

      var3 = this.reader.read(var1, var2, var3);
      if (var3 >= 0) {
        this.remaining -= var3;
        if (this.remaining == 0 && !lfFound) {
          if (var1[var3 - 1] != LF) {
            lfFound = true;
            this.remaining += 100;
          }
        } else if (lfFound) {
          for (int i = 0; i < var3; i++) {
            if (var1[i] == LF) {
              this.remaining = 0;
              return i + 1;
            }
          }
          this.remaining += 100;
        }
      }

      return var3;
    }
  }

  public long skip(long var1) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override public void mark(int readAheadLimit) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  public void close() throws IOException {
    this.reader.close();
  }

  public void setLimit(long var1) {
    this.remaining = var1;
  }

  public final long getRemaining() {
    return this.remaining;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbon.flink;

import java.io.IOException;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

public final class ProxyRecoverableOutputStream extends RecoverableFsDataOutputStream {

  ProxyRecoverableOutputStream() {
    // protected constructor.
  }

  private ProxyFileWriter<?> writer;

  public void bind(final ProxyFileWriter<?> writer) {
    if (writer == null) {
      throw new IllegalArgumentException("Argument [writer] is null.");
    }
    if (this.writer != null) {
      throw new IllegalStateException("The writer was bound.");
    }
    this.writer = writer;
  }

  @Override
  public long getPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecoverableWriter.ResumeRecoverable persist() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(final int aByte) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // TODO streaming结束的时候和出异常的时候都会调用该方法
    if (this.writer != null) {
      this.writer.close();
    }
  }

  @Override
  public Committer closeForCommit() {
    if (this.writer == null) {
      throw new IllegalStateException("The writer was not bound.");
    }
    return new Committer(
        new ProxyRecoverable(
            this.writer.getFactory().getType(),
            this.writer.getFactory().getConfiguration(),
            this.writer.getIdentifier(),
            this.writer.getPath()
        )
    );
  }

  static final class Committer implements RecoverableFsDataOutputStream.Committer {

    Committer(final ProxyRecoverable recoverable) {
      this.recoverable = recoverable;
    }

    private final ProxyRecoverable recoverable;

    @Override
    public void commit() throws IOException {
      this.newWriter().commit();
    }

    @Override
    public void commitAfterRecovery() {
      // to do nothing.
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
      return this.recoverable;
    }

    private ProxyFileWriter<?> newWriter() throws IOException {
      final ProxyFileWriterFactory writerFactory =
          ProxyFileWriterFactory.newInstance(this.recoverable.getWriterType());
      if (writerFactory == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      writerFactory.setConfiguration(this.recoverable.getWriterConfiguration());
      return writerFactory.create(this.recoverable.getWriterIdentifier(),
          this.recoverable.getWritePath());
    }

  }

}

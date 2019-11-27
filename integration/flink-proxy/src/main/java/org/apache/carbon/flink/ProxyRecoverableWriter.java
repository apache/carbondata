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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public final class ProxyRecoverableWriter implements RecoverableWriter {

  @SuppressWarnings("unchecked")
  @Override
  public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
    final SimpleVersionedSerializer<? extends CommitRecoverable> serializer =
        ProxyRecoverableSerializer.INSTANCE;
    return (SimpleVersionedSerializer<CommitRecoverable>) serializer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
    final SimpleVersionedSerializer<? extends ResumeRecoverable> serializer =
        ProxyRecoverableSerializer.INSTANCE;
    return (SimpleVersionedSerializer<ResumeRecoverable>) serializer;
  }

  @Override
  public ProxyRecoverableOutputStream open(final Path path) {
    return new ProxyRecoverableOutputStream();
  }

  @Override
  public RecoverableFsDataOutputStream recover(final ResumeRecoverable resumeRecoverable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean requiresCleanupOfRecoverableState() {
    return false;
  }

  @Override
  public boolean cleanupRecoverableState(final ResumeRecoverable resumeRecoverable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProxyRecoverableOutputStream.Committer recoverForCommit(
        final CommitRecoverable commitRecoverable
  ) {
    if (!(commitRecoverable instanceof ProxyRecoverable)) {
      throw new IllegalArgumentException(
          "ProxyFileSystem can not recover recoverable for other file system. " + commitRecoverable
      );
    }
    return new ProxyRecoverableOutputStream.Committer((ProxyRecoverable) commitRecoverable);
  }

  @Override
  public boolean supportsResume() {
    return false;
  }

}

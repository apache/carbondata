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

import java.net.URI;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

/**
 * Flink need user provides file output stream when use StreamingFileSink,
 * but CarbonWriter encapsulate the file output stream inside the SDK,
 * so, we need a proxy file output stream to connect StreamingFileSink and CarbonWriter.
 */
public final class ProxyFileSystem extends FileSystem {

  public static final String SCHEMA = "proxy";

  public static final URI DEFAULT_URI = URI.create(SCHEMA + ":/");

  public static final ProxyFileSystem INSTANCE = new ProxyFileSystem();

  private ProxyFileSystem() {
    // private constructor.
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Path getHomeDirectory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public URI getUri() {
    return DEFAULT_URI;
  }

  @Override
  public FileStatus getFileStatus(final Path path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(
          final FileStatus fileStatus,
          final long offset,
          final long length
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataInputStream open(final Path path, final int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataInputStream open(final Path path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(final Path path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(final Path path, final boolean b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean mkdirs(final Path path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream create(final Path path, final WriteMode writeMode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProxyRecoverableWriter createRecoverableWriter() {
    return new ProxyRecoverableWriter();
  }

  @Override
  public boolean rename(final Path source, final Path target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDistributedFS() {
    return false;
  }

  @Override
  public FileSystemKind getKind() {
    return FileSystemKind.FILE_SYSTEM;
  }

}

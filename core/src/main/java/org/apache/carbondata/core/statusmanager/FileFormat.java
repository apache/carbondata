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

package org.apache.carbondata.core.statusmanager;

import java.io.Serializable;
import java.util.Objects;

/**
 * The data file format supported in carbondata project
 */
public class FileFormat implements Serializable {

  public static final FileFormat COLUMNAR_V3 = new FileFormat("COLUMNAR_V3", 0);
  public static final FileFormat ROW_V1 = new FileFormat("ROW_V1", 1);

  private String format;

  private int ordinal;

  public FileFormat(String format) {
    this.format = format.toLowerCase();
  }

  public FileFormat(String format, int ordinal) {
    this.format = format.toLowerCase();
    this.ordinal = ordinal;
  }

  public int ordinal() {
    return ordinal;
  }

  public static FileFormat getByOrdinal(int ordinal) {
    if (ordinal < 0) {
      throw new IllegalArgumentException("Argument [ordinal] is less than 0.");
    }
    switch (ordinal) {
      case 0:
        return COLUMNAR_V3;
      case 1:
        return ROW_V1;
    }

    return COLUMNAR_V3;
  }

  public boolean isCarbonFormat() {
    return (this.format.equalsIgnoreCase("COLUMNAR_V3") ||
        this.format.equalsIgnoreCase("ROW_V1"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileFormat that = (FileFormat) o;
    return Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format);
  }

  @Override
  public String toString() {
    return format;
  }
}

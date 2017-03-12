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

package org.apache.carbondata.hive;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;

public class CarbonStorageFormatDescriptor extends AbstractStorageFormatDescriptor {

  @Override
  public Set<String> getNames() {
    return ImmutableSet.of("CARBONDATA");
  }

  @Override
  public String getInputFormat() {
    return MapredCarbonInputFormat.class.getName();
  }

  @Override
  public String getOutputFormat() {
    return MapredCarbonOutputFormat.class.getName();
  }

  @Override
  public String getSerde() {
    return CarbonHiveSerDe.class.getName();
  }

}

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

package org.apache.carbondata.presto;

import com.facebook.presto.spi.predicate.Domain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Objects;
import java.util.Optional;

//import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Objects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CarbondataColumnConstraint {
  private final String name;
  private final boolean invertedindexed;
  private Optional<Domain> domain;

  @JsonCreator public CarbondataColumnConstraint(@JsonProperty("name") String name,
      @JsonProperty("domain") Optional<Domain> domain,
      @JsonProperty("invertedindexed") boolean invertedindexed) {
    this.name = requireNonNull(name, "name is null");
    this.invertedindexed = requireNonNull(invertedindexed, "invertedIndexed is null");
    this.domain = requireNonNull(domain, "domain is null");
  }

  @JsonProperty public boolean isInvertedindexed() {
    return invertedindexed;
  }

  @JsonProperty public String getName() {
    return name;
  }

  @JsonProperty public Optional<Domain> getDomain() {
    return domain;
  }

  @JsonSetter public void setDomain(Optional<Domain> domain) {
    this.domain = domain;
  }

  @Override public int hashCode() {
    return Objects.hash(name, domain, invertedindexed);
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    CarbondataColumnConstraint other = (CarbondataColumnConstraint) obj;
    return Objects.equals(this.name, other.name) && Objects.equals(this.domain, other.domain)
        && Objects.equals(this.invertedindexed, other.invertedindexed);
  }

  @Override public String toString() {
    return toStringHelper(this).add("name", this.name).add("invertedindexed", this.invertedindexed)
        .add("domain", this.domain).toString();
  }
}

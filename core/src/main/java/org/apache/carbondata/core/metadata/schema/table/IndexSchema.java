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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.metadata.schema.index.IndexProperty;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.INDEX_COLUMNS;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;

/**
 * It is the new schema of index and it has less fields compare to {{@link IndexSchema}}
 */
public class IndexSchema implements Serializable, Writable {

  private static final long serialVersionUID = -8394577999061329687L;

  protected String indexName;

  /**
   * Cg and Fg Index: provider name is class name of implementation class of IndexFactory
   */
  // the old version the field name for providerName was className, so to de-serialization
  // old schema provided the old field name in the alternate filed using annotation
  @SerializedName(value = "providerName", alternate = "className")
  protected String providerName;

  /**
   * For MV, this is the identifier of the MV table.
   * For Index, this is the identifier of the main table.
   */
  protected RelationIdentifier relationIdentifier;

  /**
   * Properties provided by user
   */
  protected Map<String, String> properties;

  /**
   * child table schema
   */
  protected TableSchema childSchema;

  public IndexSchema(String indexName, String providerName) {
    this.indexName = indexName;
    this.providerName = providerName;
  }

  public IndexSchema() {
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getProviderName() {
    return providerName;
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  public void setRelationIdentifier(RelationIdentifier relationIdentifier) {
    this.relationIdentifier = relationIdentifier;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  /**
   * Return true if this is an Index
   * @return
   */
  public boolean isIndex() {
    return true;
  }

  /**
   * Return true if this index is lazy (created with DEFERRED REBUILD syntax)
   */
  public boolean isLazy() {
    String deferredRebuild = getProperties().get(IndexProperty.DEFERRED_REBUILD);
    return deferredRebuild != null && deferredRebuild.equalsIgnoreCase("true");
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(indexName);
    out.writeUTF(providerName);
    boolean isRelationIdentifierExists = null != relationIdentifier;
    out.writeBoolean(isRelationIdentifierExists);
    if (isRelationIdentifierExists) {
      this.relationIdentifier.write(out);
    }
    boolean isChildSchemaExists = null != this.childSchema;
    out.writeBoolean(isChildSchemaExists);
    if (isChildSchemaExists) {
      this.childSchema.write(out);
    }
    if (properties == null) {
      out.writeShort(0);
    } else {
      out.writeShort(properties.size());
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeUTF(entry.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.indexName = in.readUTF();
    this.providerName = in.readUTF();
    boolean isRelationIdentifierExists = in.readBoolean();
    if (isRelationIdentifierExists) {
      this.relationIdentifier = new RelationIdentifier(null, null, null);
      this.relationIdentifier.readFields(in);
    }
    boolean isChildSchemaExists = in.readBoolean();
    if (isChildSchemaExists) {
      this.childSchema = new TableSchema();
      this.childSchema.readFields(in);
    }

    int mapSize = in.readShort();
    this.properties = new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      this.properties.put(key, value);
    }
  }

  /**
   * Return the list of column name
   */
  public String[] getIndexColumns()
      throws MalformedIndexCommandException {
    String columns = getProperties().get(INDEX_COLUMNS);
    if (columns == null) {
      columns = getProperties().get(INDEX_COLUMNS.toLowerCase());
    }
    if (columns == null) {
      throw new MalformedIndexCommandException(INDEX_COLUMNS + " INDEXPROPERTY is required");
    } else if (StringUtils.isBlank(columns)) {
      throw new MalformedIndexCommandException(INDEX_COLUMNS + " INDEXPROPERTY is blank");
    } else {
      return columns.split(",", -1);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexSchema that = (IndexSchema) o;
    return Objects.equals(indexName, that.indexName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexName);
  }

}

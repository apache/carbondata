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

package org.apache.carbondata.core.metadata.datatype;

import java.lang.reflect.Type;

import org.apache.carbondata.core.util.DataTypeUtil;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * This class will deserialize DataType json string based on name
 */
public class DataTypeDeserializer implements JsonDeserializer<DataType> {
  @Override
  public DataType deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    // deserialize dataType object based on dataType name
    JsonElement jsonNameElement = jsonObject.get("name");
    if (null != jsonNameElement) {
      return context
          .deserialize(jsonObject, DataTypeUtil.valueOf(jsonNameElement.getAsString()).getClass());
    }
    return context.deserialize(jsonObject, DataType.class);
  }
}

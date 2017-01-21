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
package org.apache.carbondata.core.dictionary.generator.key;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoRegister {
  /**
   * deserialize byte to DictionaryKey
   *
   * @param bb
   * @return
   */
  public static DictionaryKey deserialize(byte[] bb) {
    Kryo kryo = new Kryo();
    kryo.setReferences(false);
    kryo.setRegistrationRequired(true);
    // register
    Registration registration = kryo.register(DictionaryKey.class);

    // deserialize
    Input input = null;
    input = new Input(bb);
    DictionaryKey key = (DictionaryKey) kryo.readObject(input, registration.getType());
    input.close();
    return key;
  }

  /**
   * serialize DictionaryKey to byte
   *
   * @param key
   * @return
   */
  public static byte[] serialize(DictionaryKey key) {
    Kryo kryo = new Kryo();
    kryo.setReferences(false);
    kryo.setRegistrationRequired(true);
    // register
    Registration registration = kryo.register(DictionaryKey.class);
    //serialize
    Output output = null;
    output = new Output(1, 4096);
    kryo.writeObject(output, key);
    byte[] bb = output.toBytes();
    output.flush();
    return bb;
  }
}

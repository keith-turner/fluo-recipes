/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.recipes.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.fluo.api.data.Bytes;

// TODO maybe put in own module so that fluo-recipes does not depend on Kryo
public class KryoSimplerSerializer implements SimpleSerializer {
  @Override
  public <T> byte[] serialize(T obj) {
    // TODO efficient object reuse (with thread safety)
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, obj);
    output.close();
    return baos.toByteArray();
  }

  @Override
  public <T> T deserialize(byte[] serObj, Class<T> clazz) {
    Kryo kryo = new Kryo();
    ByteArrayInputStream bais = new ByteArrayInputStream(serObj);
    Input input = new Input(bais);
    if (clazz.equals(Bytes.class)) {
      return (T) kryo.readObject(input, Bytes.of("").getClass());
    }
    return kryo.readObject(input, clazz);
  }

}

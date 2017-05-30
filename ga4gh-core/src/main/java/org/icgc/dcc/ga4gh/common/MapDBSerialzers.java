/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.icgc.dcc.ga4gh.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class MapDBSerialzers {

  @SneakyThrows
  public static <T> void serializeArray(DataOutput2 out, Serializer<T> serializer, T[] objects){
    out.packInt(objects.length);
    for (val object : objects){
      serializer.serialize(out, object);
    }
  }

  @SneakyThrows
  public static <T> void serializeList(DataOutput2 out, Serializer<T> serializer, List<T> objects){
    val n = objects.size();
    out.packInt(n);
    for (val object : objects){
      serializer.serialize(out, object);
    }
  }

  @SneakyThrows
  public static byte[][] deserializeDoubleByteArray(DataInput2 in, int available, Serializer<byte[]> serializer){
    val array = new byte[in.unpackInt()][];
    for (int i=0;i<array.length;i++){
      array[i] = serializer.deserialize(in,available);
    }
    return array;
  }
  @SneakyThrows
  public static <T> T[] deserializeArray(DataInput2 in, int available, Serializer<T> serializer){
    T[] array = (T[])new Object[in.unpackInt()];
    for (int i=0;i<array.length;i++){
      array[i] = serializer.deserialize(in,available);
    }
    return array;
  }

  @SneakyThrows
  public static <T> T[][] deserializeDoubleArray(DataInput2 in, int available, Serializer<T[]> serializer){
    T[][] array = (T[][])new Object[in.unpackInt()][];
    for (int i=0;i<array.length;i++){
      array[i] = serializer.deserialize(in,available);
    }
    return array;
  }

  @SneakyThrows
  public static <T> List<T> deserializeList(DataInput2 in, int available, Serializer<T> serializer){
    val list = Lists.<T>newArrayList();
    val size = in.unpackInt();
    for (int i=0;i<size;i++){
      val object = serializer.deserialize(in,available);
      list.add(object);
    }
    return list;
  }

  @RequiredArgsConstructor
  public static class StringObjectMapSerializer implements Serializer<Map<String, Object>>, Serializable {

    private static final ObjectSerializer OBJECT_SERIALIZER = new ObjectSerializer();

    @Override
    public void serialize(@NonNull DataOutput2 out, @NonNull Map<String, Object> value)
        throws IOException {
      val numKeys = value.keySet().size();
      //Write number of keys
      out.packInt(numKeys);
      for (val key : value.keySet()){
        //Write key
        out.writeUTF(key);
        OBJECT_SERIALIZER.serialize(out, value.get(key));
      }
    }

    @Override
    @SneakyThrows
    public Map<String, Object> deserialize(@NonNull DataInput2 input, int x) throws IOException {
      val map = Maps.<String, Object>newHashMap();
      //Read number of keys
      val numKeys = input.unpackInt();
      for (int i =0; i< numKeys; i++){
        //Read key
        val key = input.readUTF();

        val object = OBJECT_SERIALIZER.deserialize(input, x);

        //Put object into map
        map.put(key, object);
      }
      return map;
    }
  }

  public static class ObjectSerializer  implements Serializer<Object>, Serializable{

    @Override
    public void serialize(@NonNull DataOutput2 dataOutput2, @NonNull Object o) throws IOException {
      val baos = new ByteArrayOutputStream();
      val oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.flush();
      val byteArray = baos.toByteArray();
      BYTE_ARRAY.serialize(dataOutput2, byteArray);
    }

    @Override
    @SneakyThrows
    public Object deserialize(@NonNull DataInput2 dataInput2, int i) throws IOException {
      val byteArray = BYTE_ARRAY.deserialize(dataInput2, i);
      val bais = new ByteArrayInputStream(byteArray);
      val ois = new ObjectInputStream(bais);
      return ois.readObject();
    }

  }

}

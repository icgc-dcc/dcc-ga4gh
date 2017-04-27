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
package org.icgc.dcc.ga4gh.common.model.es;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import org.jetbrains.annotations.NotNull;
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

import static org.icgc.dcc.common.core.util.Joiners.COLON;

/*
 * ObjectNode is a bit heavy, this is just to minimize memory usage
 */
@Builder
@Value
public class EsCall implements EsModel {

  public static final String TYPE_NAME = "call";

  private int variantSetId;
  private int callSetId;
  private String callSetName;
  private Map<String, Object> info;
  private double genotypeLikelihood;
  private boolean isGenotypePhased;
  private List<Integer> nonReferenceAlleles;

  // TODO: not unique. Need to make unique or change the rule
  @Override
  public String getName() {
    return COLON.join(
        variantSetId, callSetName);
  }

  @RequiredArgsConstructor
  public static class StringObjectMapSerializer implements Serializer<Map<String, Object>>, Serializable{

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Map<String, Object> value)
        throws IOException {
      val numKeys = value.keySet().size();
      //Write number of keys
      out.packInt(numKeys);
      for (val key : value.keySet()){
        //Serialize object
        val object = value.get(key);
        val baos = new ByteArrayOutputStream();
        val oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.flush();

        //Write key
        out.writeUTF(key);

        //Write value as byte array
        val byteArray = baos.toByteArray();
        BYTE_ARRAY.serialize(out, byteArray);
      }
    }

    @Override
    @SneakyThrows
    public Map<String, Object> deserialize(@NotNull DataInput2 input, int x) throws IOException {
      val map = Maps.<String, Object>newHashMap();
      //Read number of keys
      val numKeys = input.unpackInt();
      for (int i =0; i< numKeys; i++){
        //Read key
        val key = input.readUTF();

        //Read value byte array
        val byteArray = BYTE_ARRAY.deserialize(input, x);

        //Convert byteArray to object
        val bais = new ByteArrayInputStream(byteArray);
        val ois = new ObjectInputStream(bais);
        val object = ois.readObject();

        //Put object into map
        map.put(key, object);
      }
      return map;
    }
  }

  public static class ObjectSerializer  implements Serializer<Object>, Serializable{

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull Object o) throws IOException {
      val baos = new ByteArrayOutputStream();
      val oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.flush();
      val byteArray = baos.toByteArray();
      BYTE_ARRAY.serialize(dataOutput2, byteArray);
    }

    @Override
    @SneakyThrows
    public Object deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      val byteArray = BYTE_ARRAY.deserialize(dataInput2, i);
      val bais = new ByteArrayInputStream(byteArray);
      val ois = new ObjectInputStream(bais);
      return ois.readObject();
    }

  }

  public static class EsCallSerializer implements Serializer<EsCall>, Serializable {
    private static final StringObjectMapSerializer STRING_OBJECT_MAP_SERIALIZER = new StringObjectMapSerializer();

    @Override
    public void serialize(DataOutput2 out, EsCall value) throws IOException {
      out.packInt(value.getVariantSetId());
      out.packInt(value.getCallSetId());
      out.writeUTF(value.getCallSetName());

      //WritingOutMap
      STRING_OBJECT_MAP_SERIALIZER.serialize(out, value.getInfo());

      out.writeDouble(value.getGenotypeLikelihood());
      out.writeBoolean(value.isGenotypePhased());

      val intArray = value.getNonReferenceAlleles().stream().mapToInt(i->i).toArray();
      Serializer.INT_ARRAY.serialize(out, intArray);
    }

    @Override
    @SneakyThrows
    public EsCall deserialize(DataInput2 input, int available) throws IOException {
      val variantSetId = input.unpackInt();
      val callSetId = input.unpackInt();
      val callSetName = input.readUTF();
      val info = STRING_OBJECT_MAP_SERIALIZER.deserialize(input,available);
      val genotypeLiklihood = input.readDouble();
      val genotypePhased = input.readBoolean();
      int[] nonReferenceAllelesArray = INT_ARRAY.deserialize(input,available);
      val list = Lists.<Integer>newArrayList();
      for (val i : nonReferenceAllelesArray){
        list.add(i);
      }
      return EsCall.builder()
          .callSetId(callSetId)
          .callSetName(callSetName)
          .genotypeLikelihood(genotypeLiklihood)
          .info(info)
          .isGenotypePhased(genotypePhased)
          .variantSetId(variantSetId)
          .nonReferenceAlleles(list)
          .build();
    }

  }

}

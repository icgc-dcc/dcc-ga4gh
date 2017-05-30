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
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import org.icgc.dcc.ga4gh.common.MapDBSerialzers;
import org.icgc.dcc.ga4gh.common.MapDBSerialzers.StringObjectMapSerializer;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.icgc.dcc.common.core.util.Joiners.COLON;

/*
 * ObjectNode is a bit heavy, this is just to minimize memory usage
 */
@Builder
@Value
public class EsBasicCall implements EsModel, Serializable {

  private static final long serialVersionUID = 1493648275L;

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
  public static class EsBasicCallListSerializer implements Serializer<List<EsBasicCall>> {

    @NonNull private final EsBasicCallSerializer esBasicCallSerializer;

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull List<EsBasicCall> esBasicCalls)
        throws IOException {
      MapDBSerialzers.serializeList(dataOutput2, esBasicCallSerializer, esBasicCalls);
    }

    @Override
    public List<EsBasicCall> deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      return MapDBSerialzers.deserializeList(dataInput2, i, esBasicCallSerializer);
    }

  }

  public static class EsBasicCallSerializer implements Serializer<EsBasicCall>, Serializable {
    private static final StringObjectMapSerializer STRING_OBJECT_MAP_SERIALIZER = new StringObjectMapSerializer();

    @Override
    public void serialize(DataOutput2 out, EsBasicCall value) throws IOException {
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
    public EsBasicCall deserialize(DataInput2 input, int available) throws IOException {
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
      return EsBasicCall.builder()
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

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

import static org.icgc.dcc.ga4gh.common.MapDBSerialzers.serializeList;

@Builder
@Value
public class EsConsensusCall implements Serializable {

  private static final long serialVersionUID = 1495542265L;

  public static final String TYPE_NAME = "call";

  private List<Integer> variantSetIds;
  private int callSetId;
  private String callSetName;
  private Map<String, Object> info;

  @RequiredArgsConstructor
  public static class EsConsensusCallListSerializer implements Serializer<List<EsConsensusCall>> {

    @NonNull private final EsConsensusCallSerializer esConsensusCallSerializer;

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull List<EsConsensusCall> esCalls)
        throws IOException {
      serializeList(dataOutput2, esConsensusCallSerializer, esCalls);
    }

    @Override
    public List<EsConsensusCall> deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      return MapDBSerialzers.deserializeList(dataInput2, i, esConsensusCallSerializer);
    }

  }

  public static class EsConsensusCallSerializer implements Serializer<EsConsensusCall>, Serializable {
    private static final StringObjectMapSerializer STRING_OBJECT_MAP_SERIALIZER = new StringObjectMapSerializer();

    @Override
    public void serialize(DataOutput2 out, EsConsensusCall value) throws IOException {
      serializeList(out, INTEGER,value.getVariantSetIds());
      out.packInt(value.getCallSetId());
      out.writeUTF(value.getCallSetName());
      STRING_OBJECT_MAP_SERIALIZER.serialize(out, value.getInfo());
    }

    @Override
    @SneakyThrows
    public EsConsensusCall deserialize(DataInput2 input, int available) throws IOException {
      val variantSetIds = MapDBSerialzers.deserializeList(input,available, INTEGER);
      val callSetId = input.unpackInt();
      val callSetName = input.readUTF();
      val info = STRING_OBJECT_MAP_SERIALIZER.deserialize(input,available);
      return EsConsensusCall.builder()
          .callSetId(callSetId)
          .callSetName(callSetName)
          .info(info)
          .variantSetIds(variantSetIds)
          .build();
    }

  }

}

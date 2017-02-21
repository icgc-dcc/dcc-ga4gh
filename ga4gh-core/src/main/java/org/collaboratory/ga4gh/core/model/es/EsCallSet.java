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
package org.collaboratory.ga4gh.core.model.es;

import com.google.common.collect.Iterables;
import lombok.Builder;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;

// ObjectNode is a bit heavy, this is just to minimize memory usage
@Builder
@Value
public final class EsCallSet implements EsModel {

  private String name;
  private String bioSampleId;

  @Singular
  private Iterable<Integer> variantSetIds;

  /*
   * Serializer needed for MapDB. Note: if EsCallSet member variables are added, removed or modified, this needs to be
   * updated
   */
  public static class EsCallSetSerializer implements Serializer<EsCallSet>, Serializable {

    @Override
    public void serialize(DataOutput2 out, EsCallSet value) throws IOException {
      out.writeUTF(value.getName());
      out.writeUTF(value.getBioSampleId());
      val size = Iterables.size(value.getVariantSetIds());
      out.writeInt(size);
      for (val variantSetId : value.getVariantSetIds()) {
        out.writeInt(variantSetId);
      }
    }

    @Override
    @SneakyThrows
    public EsCallSet deserialize(DataInput2 input, int available) throws IOException {
      val name = input.readUTF();
      val bioSampleId = input.readUTF();
      val size = input.readInt();
      val callSetBuilder = EsCallSet.builder()
          .name(name)
          .bioSampleId(bioSampleId);
      for (int i = 0; i < size; i++) {
        callSetBuilder.variantSetId(input.readInt());
      }
      return callSetBuilder.build();
    }

  }
}

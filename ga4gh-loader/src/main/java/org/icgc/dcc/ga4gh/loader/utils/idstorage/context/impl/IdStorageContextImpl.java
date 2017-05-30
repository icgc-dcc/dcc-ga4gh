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

package org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.List;

@Value
@RequiredArgsConstructor
public class IdStorageContextImpl<ID, K> implements IdStorageContext<ID, K> {

  @NonNull private final ID id;
  private List<K> objects = Lists.newArrayList();

  @Override public void add(K object){
    objects.add(object);
  }

  @Override public void addAll(List<K> objects){
    this.objects.addAll(objects);
  }

  public static <ID, K> IdStorageContextImpl<ID, K> createIdStorageContext(ID id) {
    return new IdStorageContextImpl<ID, K>(id);
  }

  @RequiredArgsConstructor
  public static class IdStorageContextImplSerializer<ID,K> implements Serializer<IdStorageContext<ID,K>> {

    public static <ID, K> IdStorageContextImplSerializer<ID, K> createIdStorageContextSerializer(
        Serializer<ID> idSerializer, Serializer<K> kSerializer) {
      return new IdStorageContextImplSerializer<ID, K>(idSerializer, kSerializer);
    }

    @NonNull private final Serializer<ID> idSerializer;
    @NonNull private final Serializer<K> kSerializer;

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull IdStorageContext<ID, K> idkIdStorageContext) throws IOException {
      idSerializer.serialize(dataOutput2, idkIdStorageContext.getId());

      val numObjects= idkIdStorageContext.getObjects().size();
      dataOutput2.packInt(numObjects);
      for (val objects : idkIdStorageContext.getObjects()){
        kSerializer.serialize(dataOutput2,objects);
      }
    }

    @Override public IdStorageContext<ID, K> deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      val id = idSerializer.deserialize(dataInput2, i);
      val numObjects = dataInput2.unpackInt();
      val out = IdStorageContextImpl.<ID,K>createIdStorageContext(id);
      for (int j=0; j<numObjects; j++){
        val object = kSerializer.deserialize(dataInput2,i);
        out.add(object);
      }
      return out;
    }
  }

}

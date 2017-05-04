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
import lombok.Singular;
import lombok.Value;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.List;

import static org.icgc.dcc.ga4gh.common.MapDBSerialzers.deserializeList;
import static org.icgc.dcc.ga4gh.common.MapDBSerialzers.serializeList;

@Builder
@Value
public class EsVariantCallPair2 {

  public static EsVariantCallPair2 createEsVariantCallPair2(EsVariant variant, List<EsCall> calls) {
    return new EsVariantCallPair2(variant, calls);
  }

  private EsVariant variant;

  @Singular
  private List<EsCall> calls;

  @RequiredArgsConstructor
  public static class EsVariantCallPairSerializer implements Serializer<EsVariantCallPair2>{

    public static EsVariantCallPairSerializer createEsVariantCallPairSerializer(
        EsVariant.EsVariantSerializer variantSerializer,
        EsCall.EsCallSerializer callSerializer) {
      return new EsVariantCallPairSerializer(variantSerializer, callSerializer);
    }

    @NonNull private final EsVariant.EsVariantSerializer variantSerializer;
    @NonNull private final EsCall.EsCallSerializer callSerializer;

    @Override
    public void serialize(@NotNull DataOutput2 dataOutput2, @NotNull EsVariantCallPair2 esVariantCallPair2) throws IOException {
      variantSerializer.serialize(dataOutput2, esVariantCallPair2.getVariant());
      serializeList(dataOutput2, callSerializer, esVariantCallPair2.getCalls());
    }

    @Override
    public EsVariantCallPair2 deserialize(@NotNull DataInput2 dataInput2, int i) throws IOException {
      val esVariant = variantSerializer.deserialize(dataInput2,i);
      val esCalls = deserializeList(dataInput2,i,callSerializer);
      return createEsVariantCallPair2(esVariant, esCalls);
    }

  }

}

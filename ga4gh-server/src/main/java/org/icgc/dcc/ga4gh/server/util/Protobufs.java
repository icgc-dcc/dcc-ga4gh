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
package org.icgc.dcc.ga4gh.server.util;

import static org.icgc.dcc.ga4gh.common.TypeChecker.isObjectCollection;
import static org.icgc.dcc.ga4gh.common.TypeChecker.isObjectMap;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import htsjdk.variant.variantcontext.CommonInfo;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class Protobufs {

  @SuppressWarnings("unchecked")
  public static ListValue createListValueFromObject(Object obj) {
    val listValueBuilder = ListValue.newBuilder();
    if (isObjectCollection(obj)) {
      for (Object elementObj : (Collection<Object>) obj) {
        listValueBuilder.addValues(Value.newBuilder().setStringValue(elementObj.toString()));
      }
    } else if (isObjectMap(obj)) {
      val map = ImmutableMap.<String, Value> builder();
      for (val entry : ((Map<?, ?>) obj).entrySet()) {
        map.put(entry.getKey().toString(), Value.newBuilder().setStringValue(entry.getValue().toString()).build());
      }
      listValueBuilder.addValues(Value.newBuilder().setStructValue(Struct.newBuilder().putAllFields(map.build())));
    } else { // Treat everything else as just a string
      listValueBuilder.addValues(Value.newBuilder().setStringValue(obj.toString()).build());
    }
    return listValueBuilder.build();
  }

  public static ListValue createListValueFromDoubles(final Iterable<Double> doubles) {
    val listValueBuilder = ListValue.newBuilder();
    for (val d : doubles) {
      listValueBuilder.addValues(
          Value.newBuilder().setNumberValue(d));
    }
    return listValueBuilder.build();
  }

  public static Map<String, ListValue> createInfo(Map<String, Object> commonInfoMap) {
    val map = ImmutableMap.<String, ListValue> builder();
    for (Map.Entry<String, Object> entry : commonInfoMap.entrySet()) {
      map.put(entry.getKey(), createListValueFromObject(entry.getValue()));
    }
    return map.build();
  }

  public static Map<String, ListValue> createInfo(CommonInfo commonInfo) {
    return createInfo(commonInfo.getAttributes());
  }
}

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

package org.icgc.dcc.ga4gh.loader;

import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.impl.RamMapStorage;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.ga4gh.common.model.es.EsCallSet.createEsCallSet;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage.createIntegerIdStorage;

@RequiredArgsConstructor
public class CallSetAccumulator {

  private final Map<String,Set<Integer>> variantSetIdsMap;
  private final Map<String, Integer> callSetIdMap;

  private int idCount = 0;

  public void addVariantSetId(String callSetName, int variantSetId){
    if (!variantSetIdsMap.containsKey(callSetName)){
      variantSetIdsMap.put(callSetName, Sets.newHashSet(variantSetId));
      callSetIdMap.put(callSetName, ++idCount);
    } else {
      val idSet = variantSetIdsMap.get(callSetName);
      idSet.add(variantSetId);
    }
  }

  public void addVariantSetIds(String callSetName, Iterable<Integer> variantSetIds){
    stream(variantSetIds).forEach(x -> addVariantSetId(callSetName, x));
  }

  public int getId(String callSetName){
    checkArgument(callSetIdMap.containsKey(callSetName), "The callSetName [%s] DNE", callSetName);
    return callSetIdMap.get(callSetName);
  }

  public EsCallSet buildEsCallSet(String callSetName){
    return createEsCallSet(callSetName,callSetName,variantSetIdsMap.get(callSetName));
  }

  public IdStorage<EsCallSet, Integer> getIdStorage(){
    return createIntegerIdStorage(getMapStorage(), idCount+1);
  }

  public MapStorage<EsCallSet, Integer> getMapStorage(){
    val mapStorage = RamMapStorage.<EsCallSet, Integer>newRamMapStorage();
    val map = mapStorage.getMap();
    for (val callSetName : variantSetIdsMap.keySet()){
      val esCallSet = buildEsCallSet(callSetName);
      val callSetId = callSetIdMap.get(callSetName);
      map.put(esCallSet, callSetId);
    }
    return mapStorage;
  }

  public Stream<EsCallSet> streamCallSets(){
    return variantSetIdsMap.keySet().stream()
        .map(this::buildEsCallSet);
  }

  public static CallSetAccumulator createCallSetAccumulator(Map<String, Set<Integer>> variantSetIdsMap,
      Map<String, Integer> callSetIdMap) {
    return new CallSetAccumulator(variantSetIdsMap, callSetIdMap);
  }


}

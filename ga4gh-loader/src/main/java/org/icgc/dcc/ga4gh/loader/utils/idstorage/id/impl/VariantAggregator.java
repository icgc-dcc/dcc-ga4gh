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

package org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;
import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.loader.utils.Purgeable;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair.createEsVariantCallPair;

@Slf4j
public class VariantAggregator implements Purgeable, Closeable {

  @NonNull private final MapStorage<EsVariant, List< EsConsensusCall>> mapStorage;
  private Map<EsVariant, List<EsConsensusCall>> map;
  private long count = 0;

  public VariantAggregator( MapStorage<EsVariant, List<EsConsensusCall>> mapStorage) {
    this.mapStorage = mapStorage;
    this.map = mapStorage.getMap();
  }

  private VariantIdContext<Long> procEntry(Map.Entry<EsVariant, List<EsConsensusCall>> entry){
    val esVariantCallPair = createEsVariantCallPair(entry.getKey(), entry.getValue());
    return VariantIdContext.<Long>createVariantIdContext(incrCount(),esVariantCallPair);
  }

  private void resetCount(){
    this.count = 0;
  }

  private long incrCount(){
    return count++;
  }

  @Override public void purge() {
    mapStorage.purge();
  }

  public void add(EsVariant esVariant, List<EsConsensusCall> esCalls) {
    esCalls.forEach(x -> add(esVariant, x  ));
  }

  public void add(EsVariant esVariant, EsConsensusCall esCall) {
    if (!map.containsKey(esVariant)){
      val callList = newArrayList(esCall);
      map.put(esVariant, callList);
    } else {
      val callList = map.get(esVariant);
      callList.add(esCall);
      map.put(esVariant, callList); //rtisma refer to JIRA ticket [https://jira.oicr.on.ca/browse/DCC-5587] -- [GA4GH] DiskMapStorage disk commit issue
    }
  }

  public Stream<VariantIdContext<Long>> streamVariantIdContext() {
    resetCount();
    return map.entrySet().stream()
        .map(this::procEntry);
  }

  @Override
  public void close() throws IOException {
    if (this.mapStorage != null){
      try {
        this.mapStorage.close();
      } catch (Throwable t){
        log.error("Could not close MapStorage [{}]", this.getClass().getName());
      }
    }
  }

  public static VariantAggregator createVariantAggregator(MapStorage<EsVariant, List<EsConsensusCall>> mapStorage) {
    return new VariantAggregator(mapStorage);
  }

}

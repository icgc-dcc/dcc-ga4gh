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

import lombok.NoArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.ga4gh.loader.Config.VARIANT_MAPDB_ALLOCATION;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CONSENSUS_CALL_LIST_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_CALL_SET_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.ES_VARIANT_SET_SERIALIZER;
import static org.icgc.dcc.ga4gh.loader.factory.Factory.RESOURCE_PERSISTED_PATH;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.IntegerIdStorage.createIntegerIdStorage;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator.createVariantAggregator;
import static org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory.createMapStorageFactory;
import static org.mapdb.Serializer.INTEGER;

@NoArgsConstructor(access = PRIVATE)
public class IdStorageFactory2 {

  //-XX:MaxDirectMemorySize=10G
  public static AbstractIdStorageTemplate<EsVariantSet, Integer>  buildVariantSetIdStorage(){
    val mapStorageFactory = createMapStorageFactory("variantSetIntegerMapStorage",
        ES_VARIANT_SET_SERIALIZER, INTEGER,
        RESOURCE_PERSISTED_PATH, -1);
    val mapStorage = mapStorageFactory.createDiskMapStorage(false);
    return createIntegerIdStorage(mapStorage, 0);
  }

  public static AbstractIdStorageTemplate<EsCallSet, Integer>  buildCallSetIdStorage(){
    val mapStorageFactory = createMapStorageFactory("callSetIntegerMapStorage",
        ES_CALL_SET_SERIALIZER, INTEGER,
        RESOURCE_PERSISTED_PATH, -1);
    val mapStorage = mapStorageFactory.createDiskMapStorage(false);
    return createIntegerIdStorage(mapStorage, 0);
  }

  public static VariantAggregator  buildVariantAggregator(){
    val mapStorageFactory = createMapStorageFactory("variantLongMapStorage",
        ES_VARIANT_SERIALIZER, ES_CONSENSUS_CALL_LIST_SERIALIZER,
        RESOURCE_PERSISTED_PATH, VARIANT_MAPDB_ALLOCATION);
    val mapStorage = mapStorageFactory.createDirectMemoryMapStorage(true);
    return createVariantAggregator(mapStorage);
  }

}

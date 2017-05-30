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

package org.icgc.dcc.ga4gh.loader.factory.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.factory.IdStorageFactory;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.LongIdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorageFactory;

@RequiredArgsConstructor
public class LongIdStorageFactory implements IdStorageFactory<Long> {

  @NonNull private final MapStorageFactory<EsVariantSet, Long>     variantSetLongMapStorageFactory;
  @NonNull private final MapStorageFactory<EsCallSet, Long>     callSetLongMapStorageFactory;

  @Override public AbstractIdStorageTemplate<EsVariantSet, Long> createVariantSetIdStorage(boolean useDisk) {
    val mapStorage = variantSetLongMapStorageFactory.createNewMapStorage(useDisk);
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Long> createCallSetIdStorage(boolean useDisk) {
    val mapStorage = callSetLongMapStorageFactory.createNewMapStorage(useDisk);
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsVariantSet, Long> persistVariantSetIdStorage() {
    val mapStorage = variantSetLongMapStorageFactory.persistMapStorage();
    return LongIdStorage.<EsVariantSet>createLongIdStorage(mapStorage, 0L);
  }

  @Override public AbstractIdStorageTemplate<EsCallSet, Long> persistCallSetIdStorage() {
    val mapStorage = callSetLongMapStorageFactory.persistMapStorage();
    return LongIdStorage.<EsCallSet>createLongIdStorage(mapStorage, 0L);
  }

  public static LongIdStorageFactory createLongIdStorageFactory(
      MapStorageFactory<EsVariantSet, Long> variantSetLongMapStorageFactory,
      MapStorageFactory<EsCallSet, Long> callSetLongMapStorageFactory) {
    return new LongIdStorageFactory( variantSetLongMapStorageFactory,
        callSetLongMapStorageFactory);
  }

}

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

import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall.EsConsensusCallBuilder;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.IdStorage;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantAggregator;

import java.io.File;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson.extractCallSetName;
import static org.icgc.dcc.ga4gh.common.model.es.EsVariantSet.createEsVariantSet;
import static org.icgc.dcc.ga4gh.loader.utils.VCF.newDefaultVCFFileReader;

@RequiredArgsConstructor
@Value
public class VcfProcessor {

  private static final EsVariantConverterJson ES_VARIANT_CONVERTER_JSON = new EsVariantConverterJson();
  private static final String NUM_CALLERS = "NumCallers";
  private static final String CALLERS = "Callers";

  @NonNull private final VariantAggregator variantAggregator;
  @NonNull private final IdStorage<EsVariantSet, Integer> variantSetIdStorage;
  @NonNull private CallSetAccumulator callSetAccumulator;
  @NonNull private final CounterMonitor callCounterMonitor;
  @NonNull private final VariantFilter variantFilter;

  /**
   * State
   */
  @NonFinal private int callSetId = 0;

  private EsConsensusCallBuilder createEsConsensusCallBuilder(PortalMetadata portalMetadata, int callSetId){
    return EsConsensusCall.builder()
        .callSetId(callSetId)
        .callSetName(portalMetadata.getSampleId());
  }

  private void processVariant(PortalMetadata portalMetadata, EsConsensusCallBuilder esCallBuilder, VariantContext variantContext){
    val esCall = convertConsensus(portalMetadata, esCallBuilder,variantContext);
    val esVariant = ES_VARIANT_CONVERTER_JSON.convertFromVariantContext(variantContext);
    variantAggregator.add(esVariant, esCall);
    callCounterMonitor.preIncr();
  }


  private EsConsensusCall convertConsensus(PortalMetadata portalMetadata, EsConsensusCallBuilder esConsensusCallBuilder,VariantContext variantContext){
    val callSetName = extractCallSetName(portalMetadata);
    //Extract callers from Info attribute
    val info = variantContext.getCommonInfo();
    checkState(info.hasAttribute(CALLERS), "[%s] attribute not found in consensus call", CALLERS);
    val callers = info.getAttributeAsList(CALLERS).stream().map(Object::toString).collect(toImmutableSet());

    // Remove info attribute as not needed anymore
    info.removeAttribute(CALLERS);
    if (info.hasAttribute(NUM_CALLERS)){
      info.removeAttribute(NUM_CALLERS);
    }

    //Build variantSets
    val variantSets = buildConsensusEsVariantSet(portalMetadata, callers);

    //Add variantSets so that any nonexisting variantSets are added to the idStorage so they are assigned an ID
    variantSets.forEach(variantSetIdStorage::add);

    //Create list of variantSetIds (integers)
    val variantSetIdList =  variantSets.stream().map(variantSetIdStorage::getId).collect(toImmutableList());

    // Add collect variantSetIds from all variants in this file, which will later be used for callSet generation
    callSetAccumulator.addVariantSetIds(callSetName,variantSetIdList);
    val callSetId = callSetAccumulator.getId(callSetName);

    // Build ConsensusCall
    return esConsensusCallBuilder
        .info(info.getAttributes())
        .variantSetIds(variantSetIdList)
        .callSetId(callSetId)
        .build();
  }

  private Set<EsVariantSet> buildConsensusEsVariantSet(PortalMetadata portalMetadata, Set<String> callers){
    val dataSetId = portalMetadata.getDataType();
    val referenceName = portalMetadata.getReferenceName();
    return callers.stream()
        .map(x ->  createEsVariantSet(x, dataSetId, referenceName))
        .collect(toImmutableSet());
  }

  public void process(PortalMetadata portalMetadata, File vcfFile){
    //Open file, and process each variant, to create variantSets and Calls
    val vcfFileReader = newDefaultVCFFileReader(vcfFile);
    val esConsensusCallBuilder = createEsConsensusCallBuilder(portalMetadata, callSetId);
    stream(vcfFileReader)
        .filter(variantFilter::passedFilter)
        .forEach(v -> processVariant(portalMetadata, esConsensusCallBuilder, v));
  }



  public static VcfProcessor createVcfProcessor(VariantAggregator variantAggregator,
      IdStorage<EsVariantSet, Integer> variantSetIdStorage,
      CallSetAccumulator callSetAccumulator, CounterMonitor callCounterMonitor,
      VariantFilter variantFilter) {
    return new VcfProcessor(variantAggregator, variantSetIdStorage, callSetAccumulator, callCounterMonitor,
        variantFilter);
  }

}

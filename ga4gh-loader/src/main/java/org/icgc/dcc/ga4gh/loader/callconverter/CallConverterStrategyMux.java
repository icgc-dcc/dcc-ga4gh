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

package org.icgc.dcc.ga4gh.loader.callconverter;

import lombok.val;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.common.types.WorkflowTypes;
import org.icgc.dcc.ga4gh.loader.callconverter.impl.EmptyCallConverterStrategy;
import org.icgc.dcc.ga4gh.loader.callconverter.impl.SingleCallConverterStrategy;

import static org.icgc.dcc.ga4gh.loader.callconverter.TumorCallConverterStrategyTypes.NT_CALL_CONVERTER_STRATEGY;
import static org.icgc.dcc.ga4gh.loader.callconverter.TumorCallConverterStrategyTypes.TUMOR_CALL_CONVERTER_STRATEGY;
import static org.icgc.dcc.ga4gh.loader.callconverter.TumorCallConverterStrategyTypes.TUMOUR_CALL_CONVERTER_STRATEGY;
import static org.icgc.dcc.ga4gh.loader.callconverter.impl.DualCallConverterStrategy.createDualCallConverterStrategy;
import static org.icgc.dcc.ga4gh.loader.callconverter.impl.FunctorTumorGenotypeClassifier.createFunctorTumorGenotypeClassifier;

public class CallConverterStrategyMux {
  private static final EmptyCallConverterStrategy EMPTY_CALL_CONVERTER_STRATEGY = new EmptyCallConverterStrategy();
  private static final SingleCallConverterStrategy SINGLE_CALL_CONVERTER_STRATEGY = new SingleCallConverterStrategy();

  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;

  public CallConverterStrategy select(PortalMetadata portalMetadata){
    val portalFilename = portalMetadata.getPortalFilename();
    val tumorAliquotId = portalFilename.getAliquotId();
    val workflowType = WorkflowTypes.parseMatch(portalFilename.getWorkflow(), F_CHECK_CORRECT_WORKTYPE);
    val uuidTumorGenotypeClassifier = createFunctorTumorGenotypeClassifier(tumorAliquotId, String::equals);
    val uuidCallConverterPos0 = createDualCallConverterStrategy(uuidTumorGenotypeClassifier, true);
    switch(workflowType){
      case CONSENSUS:
        return EMPTY_CALL_CONVERTER_STRATEGY;
      case BROAD_SNOWMAN:
        return NT_CALL_CONVERTER_STRATEGY;
      case BROAD_SNOWMAN_10:
        return NT_CALL_CONVERTER_STRATEGY;
      case BROAD_SNOWMAN_11:
        return NT_CALL_CONVERTER_STRATEGY;
      case BROAD_SNOWMAN_13:
        return NT_CALL_CONVERTER_STRATEGY;
      case BROAD_SNOWMAN_14:
        return NT_CALL_CONVERTER_STRATEGY;
      case BROAD_MUTECT_V3:
        return SINGLE_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_2:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_3:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_4:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_5:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_6:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_7:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case SVCP_1_0_8:
        return TUMOUR_CALL_CONVERTER_STRATEGY;
      case MUSE_1_0RC_B391201_VCF: //UUID left
        return uuidCallConverterPos0;
      case MUSE_1_0RC_VCF: //UUID left
        return uuidCallConverterPos0;
      case DKFZ_SNVCALLING_1_0_132_1:
        return TUMOR_CALL_CONVERTER_STRATEGY;
      case DKFZ_SNVCALLING_1_0_132_1_HPC:
        return TUMOR_CALL_CONVERTER_STRATEGY;
      case DKFZ_INDELCALLING_1_0_132_1:
        return TUMOR_CALL_CONVERTER_STRATEGY;
      case DKFZ_INDELCALLING_1_0_132_1_HPC:
        return TUMOR_CALL_CONVERTER_STRATEGY;
      default:
        throw new IllegalStateException(String.format("No CallConverterStrategy for the workflowType [%s]", workflowType.name()));
    }

  }

}

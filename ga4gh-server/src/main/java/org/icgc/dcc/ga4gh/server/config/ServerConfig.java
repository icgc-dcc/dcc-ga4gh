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
package org.icgc.dcc.ga4gh.server.config;

import lombok.val;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsConsensusCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.types.IndexModes;
import org.icgc.dcc.ga4gh.server.reference.ReferenceGenome;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.icgc.dcc.ga4gh.server.Factory.newClient;

@Configuration
public class ServerConfig {

  public static final String INDEX_NAME = getProperty("index_name", "dcc-variants");
  public static final String NODE_ADDRESS = getProperty("node_address", "localhost");
  public static final int NODE_PORT = parseInt(getProperty("node_port", "9300"));
  public static final String FASTA_FILE_LOC = "target/GRCh37.fasta";
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(5);
  public static final int DEFAULT_PAGE_SIZE  = 10;
  public static final IndexModes INDEX_MODE = IndexModes.valueOf(getProperty("index_mode", "NESTED"));

  @Bean
  public ReferenceGenome referenceGenome(@Value("${reference.fastaFile:" + FASTA_FILE_LOC + "}") String fastaFile) {
    return new ReferenceGenome(fastaFile);
  }

  @Bean
  public Client client() {
    return newClient();
  }

  @Bean
  public EsVariantSetConverterJson esVariantSetConverter(){
    return new EsVariantSetConverterJson();
  }

  @Bean
  public EsCallSetConverterJson esCallSetConverter(){
    return new EsCallSetConverterJson();
  }

  @Bean
  public EsConsensusCallConverterJson esConsensusCallConverterJson(){
    return new EsConsensusCallConverterJson();
  }

  @Bean
  public EsVariantCallPairConverterJson esVariantCallPairConverterJson() {
    val varConv = new EsVariantConverterJson();
    val callConv = new EsConsensusCallConverterJson();
    return new EsVariantCallPairConverterJson(varConv, callConv, varConv, callConv);
  }

  @Bean
  public IndexModes indexMode(){
    return INDEX_MODE;
  }

}

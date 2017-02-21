/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
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
package org.collaboratory.ga4gh.server.config;

import org.collaboratory.ga4gh.core.model.converters.EsCallConverter;
import org.collaboratory.ga4gh.core.model.converters.EsCallSetConverter;
import org.collaboratory.ga4gh.core.model.converters.EsVariantConverter;
import org.collaboratory.ga4gh.core.model.converters.EsVariantSetConverter;
import org.collaboratory.ga4gh.server.reference.ReferenceGenome;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.collaboratory.ga4gh.server.Factory.newClient;

@Configuration
public class ServerConfig {

  public static final String INDEX_NAME = getProperty("index_name", "dcc-variants");
  public static final String NODE_ADDRESS = getProperty("node_address", "localhost");
  public static final int NODE_PORT = parseInt(getProperty("node_port", "9300"));
  public static final String FASTA_FILE_LOC = "target/GRCh37.fasta";

  public static String toConfigString() {
    return String.format(
        "INDEX_NAME: %s"
            + "\nNODE_ADDRESS: %s"
            + "\nNODE_PORT: %s",
        INDEX_NAME,
        NODE_ADDRESS,
        NODE_PORT);
  }

  @Bean
  public ReferenceGenome referenceGenome(@Value("${reference.fastaFile:" + FASTA_FILE_LOC + "}") String fastaFile) {
    return new ReferenceGenome(fastaFile);
  }

  @Bean
  public Client client() {
    return newClient();
  }

  @Bean
  public EsVariantConverter esVariantConverter(){
    return new EsVariantConverter();
  }

  @Bean
  public EsVariantSetConverter esVariantSetConverter(){
    return new EsVariantSetConverter();
  }

  @Bean
  public EsCallSetConverter esCallSetConverter(){
    return new EsCallSetConverter();
  }

  @Bean
  public EsCallConverter esCallConverter(){
    return new EsCallConverter();
  }

}

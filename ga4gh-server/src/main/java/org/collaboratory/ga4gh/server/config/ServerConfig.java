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

import org.collaboratory.ga4gh.server.Factory;
import org.collaboratory.ga4gh.server.reference.ReferenceGenome;
import org.elasticsearch.client.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServerConfig {

  public static final String INDEX_NAME = "dcc-variants";
  public static final String VARIANT_TYPE_NAME = "variant";
  public static final String VARIANT_SET_TYPE_NAME = "variant_set";
  public static final String CALLSET_TYPE_NAME = "callset";
  public static final String CALL_TYPE_NAME = "call";
  public static final String HEADER_TYPE_NAME = "vcf_header";
  public static final String NODE_ADDRESS = System.getProperty("node_address", "localhost");
  public static final int NODE_PORT = Integer.valueOf(System.getProperty("node_port", "9300"));
  public static final String FASTA_FILE_LOC = "target/GRCh37.fasta";

  public static String toConfigString() {
    StringBuilder sb = new StringBuilder();
    sb.append("INDEX_NAME: " + INDEX_NAME);
    sb.append("\nTYPE_NAME: " + VARIANT_TYPE_NAME);
    sb.append("\nNODE_ADDRESS: " + NODE_ADDRESS);
    sb.append("\nNODE_PORT: " + NODE_PORT);
    return sb.toString();
  }

  @Bean
  public ReferenceGenome referenceGenome(@Value("${reference.fastaFile:" + FASTA_FILE_LOC + "}") String fastaFile) {
    return new ReferenceGenome(fastaFile);
  }

  @Bean
  public Client client() {
    return Factory.newClient();
  }

}

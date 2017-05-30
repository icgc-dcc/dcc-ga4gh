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
package org.icgc.dcc.ga4gh.loader.indexing;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import org.elasticsearch.client.Client;

import java.util.List;

@Builder
@Value
public class IndexCreatorContext {

  /*
   * Es Client handle
   */
  @NonNull
  private final Client client;

  /*
   * Name of index
   */
  @NonNull
  private final String indexName;

  /*
   * Directory name where mapping files is stored
   */
  @NonNull
  private final String mappingDirname;

  /*
   * Extension that is appended to the end of the generated mapping file filename
   */
  @NonNull
  private final String mappingFilenameExtension;

  /*
   * Filename where index setting file is stored, relative to MappingDirname
   */
  @NonNull
  private final String indexSettingsFilename;

  /*
   * Enable bit, used to enable or disable indexing
   */
  private final boolean indexingEnabled;

  /*
   * MiscNames of the different types in this index
   */
  @NonNull
  @Singular
  private final List<String> typeNames;

}

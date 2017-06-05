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

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.NonNull;
import org.elasticsearch.client.Client;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@Data
public class IndexCreatorContext {

  private final Map<String, Path> typeMap = Maps.newHashMap();
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
   * Filename where index setting file is stored, relative to MappingDirname
   */
  @NonNull
  private final Path indexSettingsPath;

  /*
   * Enable bit, used to enable or disable indexing
   */
  private final boolean indexingEnabled;

  /**
   * Maps a typeName to a file path
   */
  public IndexCreatorContext add(String typeName, Path mappingFilePath){
    typeMap.put(typeName, mappingFilePath);
    return this;
  }

  public Set<String> getTypeNames(){
    return typeMap.keySet();
  }

  public Path getPath(String typeName){
    checkArgument(typeMap.containsKey(typeName), "The typeName [%s] DNE", typeName);
    return typeMap.get(typeName);
  }

  public static IndexCreatorContext createIndexCreatorContext(Client client, String indexName, Path indexSettingsPath,
      boolean indexingEnabled) {
    return new IndexCreatorContext(client, indexName, indexSettingsPath, indexingEnabled);
  }

}

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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

/*
 * Prepares an index, by reading a configuration object to properly index the types and 
 * to know where to read the mapping files from.
 */
@RequiredArgsConstructor
@Slf4j
public class IndexCreator {

  @NonNull
  private final IndexCreatorContext indexCreatorContext;

  /*
   * Executes the indexing based on the configuration
   */
  public void execute() throws ExecutionException, InterruptedException {

    if (indexCreatorContext.isIndexingEnabled()) {

      val client = indexCreatorContext.getClient();
      val indexName = indexCreatorContext.getIndexName();

      log.info("Preparing index {}...", indexName);
      val indexes = client.admin().indices();
      val doesIndexExist = indexes.prepareExists(indexName).execute().get().isExists();
      if (doesIndexExist) {
        checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
        log.info("Deleted existing [{}] index", indexName);
      }

      /// indexes.preparePutMapping(indices)
      val createIndexRequestBuilder = indexes.prepareCreate(indexName)
          .setSettings(read(indexCreatorContext.getIndexSettingsPath()).toString());
      for (val typeName : indexCreatorContext.getTypeNames()){
        addMapping(createIndexRequestBuilder, typeName);
      }
      checkState(createIndexRequestBuilder.execute().actionGet().isAcknowledged());
      log.info("Created new index [{}]", indexName);
    } else {
      log.info("Preparation of index is disabled. Skipping index creation...");
    }
  }

  @SneakyThrows
  private ObjectNode read(final Path path) {
    val url = Resources.getResource(path.toString());
    return (ObjectNode) DEFAULT.readTree(url);
  }

  private void addMapping(@NonNull final CreateIndexRequestBuilder builder, @NonNull final String typeName) {
    builder.addMapping(typeName, read(indexCreatorContext.getPath(typeName)).toString());
  }


}

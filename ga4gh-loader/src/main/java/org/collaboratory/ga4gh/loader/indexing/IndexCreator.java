package org.collaboratory.ga4gh.loader.indexing;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/*
 * Prepares an index, by reading a configuration object to properly index the types and 
 * to know where to read the mapping files from.
 */
@RequiredArgsConstructor
@Slf4j
public class IndexCreator {

  @NonNull
  private final IndexConfiguration indexerConfiguration;

  /*
   * Executes the indexing based on the configuration
   */
  public void execute() throws ExecutionException, InterruptedException {

    if (indexerConfiguration.isIndexingEnabled()) {

      val client = indexerConfiguration.getClient();
      val indexName = indexerConfiguration.getIndexName();

      log.info("Preparing index {}...", indexName);
      val indexes = client.admin().indices();
      val doesIndexExist = indexes.prepareExists(indexName).execute().get().isExists();
      if (doesIndexExist) {
        checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
        log.info("Deleted existing [{}] index", indexName);
      }

      val createIndexRequestBuilder = indexes.prepareCreate(indexName)
          .setSettings(read(indexerConfiguration.getIndexSettingsFilename()).toString());

      for (val typeName : indexerConfiguration.getTypeNames()) {
        addMapping(createIndexRequestBuilder, typeName);
      }
      checkState(createIndexRequestBuilder.execute().actionGet().isAcknowledged());
      log.info("Created new index [{}]", indexName);
    } else {
      log.info("Preparation of index is disabled. Skipping index creation...");
    }
  }

  @SneakyThrows
  private ObjectNode read(final String fileName) {
    val url = Resources.getResource(indexerConfiguration.getMappingDirname() + "/" + fileName);
    return (ObjectNode) DEFAULT.readTree(url);
  }

  private void addMapping(@NonNull final CreateIndexRequestBuilder builder, @NonNull final String typeName) {
    builder.addMapping(typeName, read(typeName + this.indexerConfiguration.getMappingFilenameExtension()).toString());
  }

}

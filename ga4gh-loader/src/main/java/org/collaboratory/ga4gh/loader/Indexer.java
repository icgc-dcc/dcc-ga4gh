package org.collaboratory.ga4gh.loader;

import static org.elasticsearch.common.base.Preconditions.checkState;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.DocumentType;
import org.icgc.dcc.dcc.common.es.model.Document;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Indexer {

	private final Client client;
	private final DocumentWriter writer;
	private final String indexName;

	private int id = 1;

	@SneakyThrows
	public void prepareIndex() {
		log.info("Preparing index {}...", indexName);
		val indexes = client.admin().indices();
		if (indexes.prepareExists(indexName).execute().get().isExists()) {
			checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
		}

		indexes.prepareCreate(indexName).execute();
	}

	@SneakyThrows
	public void indexVariants(@NonNull Iterable<ObjectNode> variants, String objectId) {
		for (val variant : variants) {
			writeVariant(variant);
		}
	}

	private void writeVariant(ObjectNode variant) throws IOException {
		writer.write(new Document(nextId(), variant, new VariantDocumentType()));
	}

	private String nextId() {
		return String.valueOf(id++);
	}

	private static class VariantDocumentType implements DocumentType {

		@Override
		public String getIndexType() {
			return Config.TYPE_NAME;
		}

	}

}

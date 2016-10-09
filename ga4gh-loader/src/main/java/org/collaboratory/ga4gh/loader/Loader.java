package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Loader {

	public static void main(String[] args) {
		try (val client = newClient(); val writer = newDocumentWriter()) {
			val loader = newLoader(client, writer);
			loader.load();
		} catch (Exception e) {
			log.error("Exception running: ", e);
		}
	}

	@NonNull
	private final Indexer indexer;

	public void load() {
		indexer.prepareIndex();

		log.info("Resolving object ids...");
		val objectIds = Portal.getObjectIds();

		for (val objectId : objectIds) {
			loadObject(objectId);
		}
	}

	private void loadObject(String objectId) {
		log.info("Downloading file {}...", objectId);
		val file = Storage.downloadFile(objectId);

		log.info("Reading variants from {}...", file);
		@Cleanup
		val vcf = new VCF(file);
		val variants = vcf.read();

		log.info("Indexing {}...", objectId);
		indexer.indexVariants(variants, objectId);
	}

}

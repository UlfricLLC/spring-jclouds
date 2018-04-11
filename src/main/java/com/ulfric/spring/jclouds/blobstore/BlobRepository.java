package com.ulfric.spring.jclouds.blobstore;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.Payload;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

@Repository
public class BlobRepository {

	private final ObjectMapper objectMapper;
	private final String containerName;
	private final String jcloudsProvider;
	private final String jcloudsId;
	private final String jcloudsSecret;
	private final BlobStoreContext context;

	public BlobRepository(ObjectMapper objectMapper,
			@Value("${BUCKET_NAME:springjclouds}") String containerName,
			@Value("${JCLOUDS_PROVIDER:transient}") String jcloudsProvider,
			@Value("${JCLOUDS_ID:none}") String jcloudsId,
			@Value("${JCLOUDS_SECRET:}") String jcloudsSecret) {
		this.objectMapper = objectMapper;
		this.containerName = containerName;
		this.jcloudsProvider = jcloudsProvider;
		this.jcloudsId = jcloudsId;
		this.jcloudsSecret = jcloudsSecret;

		this.context = createBucket();
	}

	private BlobStoreContext createBucket() {
		BlobStoreContext context = ContextBuilder.newBuilder(jcloudsProvider)
				.credentials(jcloudsId, jcloudsSecret.replace("\\n", "\n"))
				.build(BlobStoreContext.class);

		getBlobStore().createContainerInLocation(null, containerName);

		return context;
	}

	public void put(String key, Object value) {
		BlobStore blobStore = getBlobStore();
		Blob blob;
		try {
			blob = blobStore.blobBuilder(key).payload(objectMapper.writeValueAsString(value)).build();
		} catch (JsonProcessingException exception) {
			throw new RuntimeException("Could not parse json", exception);
		}
		blobStore.putBlob(containerName, blob);
	}

	public <T> T get(String key, Class<T> type) {
		Blob blob = getBlobStore().getBlob(containerName, key);
		return translate(blob, type);
	}

	public void delete(String key) {
		getBlobStore().removeBlob(containerName, key);
	}

	public <T> List<T> list(String bucket, Class<T> type) {
		return list(bucket, type, null, getBlobStore());
	}

	private <T> List<T> list(String bucket, Class<T> type, String afterMarker, BlobStore blobStore) {
		ListContainerOptions options = ListContainerOptions.Builder
				.afterMarker(afterMarker)
				.prefix(bucket)
				.delimiter("/");

		PageSet<? extends StorageMetadata> pages = blobStore.list(containerName, options);
		List<T> values = pages.stream()
			.filter(meta -> meta.getType() == StorageType.BLOB)
			.map(meta -> blobStore.getBlob(containerName, meta.getName()))
			.map(blob -> translate(blob, type))
			.collect(Collectors.toList());

		String next = pages.getNextMarker();
		if (!StringUtils.isEmpty(next)) {
			values.addAll(list(bucket, type, next, blobStore));
		}

		return values;
	}

	private <T> T translate(Blob blob, Class<T> type) {
		if (blob == null) {
			return null;
		}

		Payload payload = blob.getPayload();
		try {
			return payload == null ? null : objectMapper.readValue(payload.openStream(), type);
		} catch (JsonSyntaxException | JsonIOException | IOException exception) {
			exception.printStackTrace(); // TODO proper error handling
			return null;
		}
	}

	public BlobStore getBlobStore() {
		return context.getBlobStore();
	}

}

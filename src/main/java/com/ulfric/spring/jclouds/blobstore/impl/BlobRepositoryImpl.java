package com.ulfric.spring.jclouds.blobstore.impl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

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
import com.ulfric.spring.jclouds.blobstore.BlobRepository;

@Repository
public class BlobRepositoryImpl implements BlobRepository {

	@Inject
	private ObjectMapper json;

	@Value("${BUCKET_NAME:springjclouds}")
	private String containerName;

	@Value("${JCLOUDS_PROVIDER:transient}")
	private String jcloudsProvider;

	@Value("${JCLOUDS_ID:none}")
	private String jcloudsId;

	@Value("${JCLOUDS_SECRET:}")
	private String jcloudsSecret;

	private BlobStoreContext context;

	@PostConstruct
	public void createBucket() {
		context = ContextBuilder.newBuilder(jcloudsProvider)
				.credentials(jcloudsId, jcloudsSecret.replace("\\n", "\n"))
				.build(BlobStoreContext.class);

		getBlobStore().createContainerInLocation(null, containerName);
	}

	@Override
	public void put(String key, Object value) {
		BlobStore blobStore = getBlobStore();
		Blob blob;
		try {
			blob = blobStore.blobBuilder(key).payload(json.writeValueAsString(value)).build();
		} catch (JsonProcessingException exception) {
			throw new RuntimeException("Could not parse json", exception);
		}
		blobStore.putBlob(containerName, blob);
	}

	@Override
	public <T> T get(String key, Class<T> type) {
		Blob blob = getBlobStore().getBlob(containerName, key);
		return translate(blob, type);
	}

	@Override
	public void delete(String key) {
		getBlobStore().removeBlob(containerName, key);
	}

	@Override
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
			return payload == null ? null : json.readValue(payload.openStream(), type);
		} catch (JsonSyntaxException | JsonIOException | IOException exception) {
			exception.printStackTrace(); // TODO proper error handling
			return null;
		}
	}

	public BlobStore getBlobStore() {
		return context.getBlobStore();
	}

}

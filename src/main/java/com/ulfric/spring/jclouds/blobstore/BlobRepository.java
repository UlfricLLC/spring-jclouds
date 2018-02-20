package com.ulfric.spring.jclouds.blobstore;

import java.util.List;

public interface BlobRepository {

	void put(String key, Object value);

	<T> T get(String key, Class<T> type);

	void delete(String key);

	<T> List<T> list(String bucket, Class<T> type);

}

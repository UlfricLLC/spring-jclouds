package com.ulfric.spring.jclouds.blobstore;

public interface BlobRepository {

	void put(String key, Object value);

	<T> T get(String key, Class<T> type);

	void delete(String key);

}

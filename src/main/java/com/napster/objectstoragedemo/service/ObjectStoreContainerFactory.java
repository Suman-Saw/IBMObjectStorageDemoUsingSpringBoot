package com.napster.objectstoragedemo.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ObjectStoreContainerFactory {
	
	@Value("${spring.objectstore.uploadBucket}")
	private String uploadBucket;

	public String getBucketName() {
		// later a full factory implementation to go in
		return uploadBucket;
	}
}

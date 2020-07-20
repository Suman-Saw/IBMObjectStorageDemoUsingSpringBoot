package com.napster.objectstoragedemo.util;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.napster.objectstoragedemo.service.ObjectStoreContainerFactory;
import com.napster.objectstoragedemo.service.ObjectStoreHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Slf4j
@Service
public class ObjectStoreUtils {

	@Autowired
	ObjectStoreHandler objectStoreHandler;

	@Autowired
	ObjectStoreContainerFactory objectStoreContainerFactory;

	public String uploadObject(File initialFile, String fileName, ObjectMetadata objectMetadata) {
		InputStream targetStream = null;
		try {
			targetStream = FileUtils.openInputStream(initialFile);
		} catch (IOException e) {
			log.error("IOException in uploadObject:ObjectStoreUtils :: " + e.toString());
		}

		String response = null;
		try {
			response = objectStoreHandler.objectStore(targetStream, fileName,
					objectStoreContainerFactory.getBucketName(), objectMetadata);
		} catch (Exception e) {
			log.error("Exception in uploadObject:ObjectStoreUtils :: " + e.toString());
		}
		return response;
	}

	public File getDownloadedFile(String fileStr) throws Exception {
		return objectStoreHandler.getFileContent(fileStr, objectStoreContainerFactory.getBucketName());
	}

	public String storeJsonString(String jsonString, String accesskey, String bucketname) {
		InputStream inputStream = new ByteArrayInputStream(jsonString.getBytes());
		ObjectMetadata omd = new ObjectMetadata();
		String retStr = "Failed";

		try {
			omd.addUserMetadata("accesskey", accesskey);
		} catch (Exception e) {
			log.error("Exception occurred while adding metadata : " + e.toString() + e.getMessage());
		}

		try {
			retStr = objectStoreHandler.objectStore(inputStream, accesskey, bucketname, omd);
		} catch (Exception e) {
			log.error("Exception occurred while fetching from object storage : " + e.toString() + e.getMessage());
		}

		try {
			inputStream.close();
		} catch (IOException e) {
			log.error("Exception occurred while closing the input stream : " + e.toString() + e.getMessage());
		}
		return retStr;
	}

	public String getCloudObjectStoreFile(String filename,String bucket) throws Exception {
		return objectStoreHandler.getFileStr(filename,bucket);
	}

	public List<String> listObjects(String bucketName){
		return objectStoreHandler.listObjects(bucketName);
	}

}

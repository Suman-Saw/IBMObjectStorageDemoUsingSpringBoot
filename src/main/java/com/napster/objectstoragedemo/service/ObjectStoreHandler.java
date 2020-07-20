package com.napster.objectstoragedemo.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.util.IOUtils;
import com.napster.objectstoragedemo.bin.AmazonS3ConnectionPoolBean;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Slf4j
@Service
public class ObjectStoreHandler {

	@Autowired
	ObjectStorePool objectStorePool;
	@Autowired
	AmazonS3ConnectionPoolBean amazonS3ConnectionPoolBean;


	public String objectStore(InputStream targetStream, String fileName, String containerName,
			ObjectMetadata objectMetadata) {
		String accessKey = fileName;
		AmazonS3 cos = null;
		PutObjectRequest request = null;
		PutObjectResult result = null;
		
		try {
			cos = objectStorePool.createClient();
			//cos = amazonS3ConnectionPoolBean.getS3ClientBean();
		} catch (Exception e) {
			log.error("Exception while creating client : " + e.toString() + " : " + e.getMessage());
		}
		
		
		try {
			request = new PutObjectRequest(containerName, accessKey, targetStream, objectMetadata);
		} catch (Exception e) {
			log.error("Exception while running PutObjectRequest : " + e.toString() + " : " + e.getMessage());
		}
		
		try {
			result = cos.putObject(request);
		} catch (Exception e) {
			log.error("Exception while putting object : " + e.toString() + " : " + e.getMessage());		
		}

		return result.getContentMd5();
	}


	public String getFileStr(String fileName, String myContainer) throws Exception {
		AmazonS3 cos = null;
		S3Object object = null;
		InputStream objectData = null;
		try {
			cos = objectStorePool.createClient();
		} catch (Exception e) {

			log.error("Exception while creating client : " + e.toString() + e.getMessage());

		}
		try {
			object = cos.getObject(new GetObjectRequest(myContainer, fileName));
		} catch (Exception e) {
			log.error("Exception while fetching object : " + e.toString() + e.getMessage());

		}
		try {
			objectData = object.getObjectContent();
		} catch (Exception e) {

			log.error("Exception while getting object content : " + e.toString() + e.getMessage());

		}
		String reply = IOUtils.toString(objectData);
		objectData.close();

		return reply;
	}


	public File getFileContent(String fileName, String myContainer) throws IOException {
		String ext = "";
		String name = "";

		if (fileName.contains(".")) {
			name = fileName.substring(0, fileName.lastIndexOf('.'));
			ext = fileName.substring(fileName.lastIndexOf('.'), fileName.length());
		} else {
			name = fileName;
		}

		// File reply = File.createTempFile(name,ext,null);
		File reply = new File(fileName);
		AmazonS3 cos = objectStorePool.createClient();
		S3Object object = cos.getObject(new GetObjectRequest(myContainer, fileName));
		InputStream objectData = object.getObjectContent();

		OutputStream outputStream = new FileOutputStream(reply);
		IOUtils.copy(objectData, outputStream);
		outputStream.close();
		objectData.close();

		return reply;
	}

	public void delete(String containerName, String objectName) {
		AmazonS3 cos = objectStorePool.createClient();
		try {
			cos.deleteObject(new DeleteObjectRequest(containerName, objectName));
		} catch (Exception e) {
			log.error("Exception while deleting object :: " + e.toString());
		}
	}


	public void StoreInObjectStorage(JSONObject docConvertResult, String fileName, String bucketName) {
		String str = "";
		String status = "";
		InputStream is = null;

		str = docConvertResult.toJSONString();

		is = new ByteArrayInputStream(str.getBytes());
		status = this.objectStore(is, fileName, bucketName, null);
	}

	/**
	 * 
	 * @param bucketObject
	 * @return
	 */
	public int deleteAll(JSONObject bucketObject) {
		String bucketName = bucketObject.get("bucketName").toString();
		int fileCount = Integer.parseInt(bucketObject.get("count").toString());
//		AmazonS3 s3Client = objectStorePool.createClient();
		AmazonS3 s3Client = amazonS3ConnectionPoolBean.getS3ClientBean();
		ObjectListing objectListing = s3Client.listObjects(bucketName);
		int count = 0;
		while (true) {
			Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
			DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName);
			List<KeyVersion> keys = new ArrayList<KeyVersion>();
			while (objIter.hasNext()) {
				KeyVersion kv = new KeyVersion(objIter.next().getKey());
				keys.add(kv);
				count++;
				// if fileCount = 0 then delete all
				if (fileCount != 0) {
					if (count >= fileCount) {
						break;
					}
				}
			}

			try {
				deleteObjectsRequest.setKeys(keys);
				s3Client.deleteObjects(deleteObjectsRequest);
			} catch (Exception e) {
				log.error("Eception while deleting object :: " + e.toString());
			}

			// If the bucket contains many objects, the listObjects() call
			// might not return all of the objects in the first listing. Check to
			// see whether the listing was truncated. If so, retrieve the next page
			// of objects and delete them.
			if (objectListing.isTruncated()) {
				objectListing = s3Client.listNextBatchOfObjects(objectListing);
			} else {
				break;
			}
		}
		log.info("Total # of items deleted = " + count);
		return count;
	}

	public int countBucketObjects(JSONObject bucketObject) {
		String bucketName = bucketObject.get("bucketName").toString();
//		AmazonS3 s3Client = objectStorePool.createClient();
		AmazonS3 s3Client = amazonS3ConnectionPoolBean.getS3ClientBean();
		ObjectListing objectListing = s3Client.listObjects(bucketName);
		int count = 0;
		
		while (true) {
			Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
			while (objIter.hasNext()) {
				String obj = objIter.next().getKey();
				count++;
			}
			// If the bucket contains many objects, the listObjects() call
			// might not return all of the objects in the first listing. Check to
			// see whether the listing was truncated. If so, retrieve the next page of
			// objects
			// and delete them.
			if (objectListing.isTruncated()) {
				objectListing = s3Client.listNextBatchOfObjects(objectListing);
			} else {
				break;
			}
		}
		log.info("Total Item Detected :: " + count);
		return count;

	}

	public List<String> listObjects(String bucketName)
	{
		AmazonS3 cos = null;
		LocalDateTime ldt = LocalDateTime.now();
		String objectStoreKey = DateTimeFormatter
				.ofPattern("yyyy-MM-dd", Locale.ENGLISH).format(ldt);
		try {
			cos = objectStorePool.createClient();
		} catch (Exception e) {
			log.error("Exception while creating client : " + e.toString() + e.getMessage());
		}
		System.out.println("Listing objects in bucket " + bucketName);

		return getBucketContents_withPrefix(cos,bucketName,objectStoreKey);
	}

	public List<String> getBucketContents_withPrefix(AmazonS3 _cos,String bucketName, String prefix) {
		System.out.printf("Retrieving bucket contents (V2) from: %s\n", bucketName);
		List<String> keyList = new ArrayList<>();
		int count = 0;
		boolean moreResults = true;
		String nextToken = "";

		while (moreResults) {
			ListObjectsV2Request request = new ListObjectsV2Request()
					.withBucketName(bucketName)
					//.withMaxKeys(maxKeys)
					.withPrefix(prefix)
					.withContinuationToken(nextToken);

			ListObjectsV2Result result = _cos.listObjectsV2(request);
			for(S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				//System.out.printf("Item: %s (%s bytes)\n", objectSummary.getKey(), objectSummary.getSize());
				System.out.printf("Item(%s): %s (%s bytes)\n",count++, objectSummary.getKey(), objectSummary.getSize());
				keyList.add(objectSummary.getKey());
			}

			if (result.isTruncated()) {
				nextToken = result.getNextContinuationToken();
				System.out.println("Next Token : " + nextToken);
				System.out.println("...More results in next batch!\n");
			}
			else {
				nextToken = "";
				moreResults = false;
			}
		}
		System.out.println("...No more results!");
		return keyList;
	}

}

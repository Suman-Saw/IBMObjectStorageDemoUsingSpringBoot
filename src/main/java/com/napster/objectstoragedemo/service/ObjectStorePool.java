package com.napster.objectstoragedemo.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.ibm.oauth.BasicIBMOAuthCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ObjectStorePool {

	@Value("${spring.objectstore.OBJECT_STORE_API_KEY}")
	private String api_key;

	@Value("${spring.objectstore.OBJECT_STORE_SERVICE_INSTANCE_ID}")
	private String service_instance_id;

	@Value("${spring.objectstore.OBJECT_STORE_ENDPOINT_URL}")
	private String endpoint_url;

	@Value("${spring.objectstore.OBJECT_STORE_LOCATION}")
	private String location;

	@Value("${spring.objectstore.OBJECT_STORE_TIMEOUT}")
	private int ObjectStoragetimeout;

	public AmazonS3 createClient() {
		log.info("api_key : " + api_key);
		log.info("service_instance_id : " + service_instance_id);
		log.info("endpoint_url : " + endpoint_url);
		log.info("location : " + location);
		return createClient(api_key, service_instance_id, endpoint_url, location);
	}

	private AmazonS3 createClient(String api_key, String service_instance_id, String endpoint_url, String location) {
		AWSCredentials credentials;
		if (endpoint_url.contains("objectstorage.softlayer.net")) {
			credentials = new BasicIBMOAuthCredentials(api_key, service_instance_id);
		} else {
			String access_key = api_key;
			String secret_key = service_instance_id;
			credentials = new BasicAWSCredentials(access_key, secret_key);
		}
		ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(ObjectStoragetimeout);
		clientConfig.setUseTcpKeepAlive(true);

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withEndpointConfiguration(new EndpointConfiguration(endpoint_url, location))
				.withPathStyleAccessEnabled(true).withClientConfiguration(clientConfig).build();
		return s3Client;
	}

}

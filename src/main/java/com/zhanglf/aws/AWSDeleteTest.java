package com.zhanglf.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AWSDeleteTest extends AwsConfig {
	public void deleteObject(String key){
		AWSCredentials credentials=new BasicAWSCredentials(accessKey,secretKey);
		ClientConfiguration clientConfig=new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);
		AmazonS3Client conn=new AmazonS3Client(credentials,clientConfig);
		conn.setEndpoint(ENDPOINT);
		conn.deleteObject(bucketName, key);
	}
	
	
	public void deleteObjectWithClientBuilder(String key){
		AWSCredentials credentials=new BasicAWSCredentials(accessKey,secretKey);
		ClientConfiguration clientConfig=new ClientConfiguration();
			clientConfig.setProtocol(Protocol.HTTP);
			AmazonS3ClientBuilder builder=AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
			EndpointConfiguration endpointConfiguration=new EndpointConfiguration(ENDPOINT,Regions.AP_NORTHEAST_1.getName());
			builder.setEndpointConfiguration(endpointConfiguration);
			
			AmazonS3 conn=builder.build();
			conn.deleteObject(bucketName, key);
	}
	
	
	
}

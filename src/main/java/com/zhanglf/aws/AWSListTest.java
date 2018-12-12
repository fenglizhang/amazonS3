package com.zhanglf.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AWSListTest extends AwsConfig {
	public void listObject(){
		AWSCredentials credentials=new BasicAWSCredentials(accessKey,secretKey);
		ClientConfiguration clientConfig=new ClientConfiguration();
		
		try {
			clientConfig.setProtocol(Protocol.HTTP);
			AmazonS3Client conn=new AmazonS3Client(credentials,clientConfig);
			conn.setEndpoint(ENDPOINT);
			
			ObjectListing objects = conn.listObjects(bucketName);
			do {
				for(S3ObjectSummary objectSummary:objects.getObjectSummaries()){
					System.out.println(objectSummary.getKey());
				}
			} while (objects.isTruncated());
		} catch (AmazonServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AmazonClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void listObjectWithClientBuilder(){
		AWSCredentials credentials=new BasicAWSCredentials(accessKey,secretKey);
		ClientConfiguration clientConfig=new ClientConfiguration();
		try {
			clientConfig.setProtocol(Protocol.HTTP);
			AmazonS3ClientBuilder builder=AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
			EndpointConfiguration endpointConfiguration=new EndpointConfiguration(ENDPOINT,Regions.AP_NORTHEAST_1.getName());
			builder.setEndpointConfiguration(endpointConfiguration);
			
			AmazonS3 conn=builder.build();
			
			ListObjectsV2Request req=new ListObjectsV2Request().withBucketName(bucketName);
			
			ListObjectsV2Result result;
			do{
				result=	conn.listObjectsV2(req);
				for(S3ObjectSummary objectSummary :result.getObjectSummaries()){
					
					System.out.println(objectSummary.getKey());
				}
			}while(result.isTruncated());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void getObjectByKey(String key){
		AWSCredentials credentials=new BasicAWSCredentials(accessKey,secretKey);
		ClientConfiguration clientConfig=new ClientConfiguration();
		try {
			clientConfig.setProtocol(Protocol.HTTP);
			AmazonS3ClientBuilder builder=AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
			EndpointConfiguration endpointConfiguration=new EndpointConfiguration(ENDPOINT,Regions.AP_NORTHEAST_1.getName());
			builder.setEndpointConfiguration(endpointConfiguration);
			
			AmazonS3 conn=builder.build();
			
			S3Object object = conn.getObject(bucketName, key);
			System.out.println(object.getKey());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}

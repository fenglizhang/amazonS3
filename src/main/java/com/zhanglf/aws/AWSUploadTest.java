package com.zhanglf.aws;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class AWSUploadTest extends AwsConfig {
	private File file = new File("D:/root/zlf/20171108/yuanzougaofei.mp3");

	/**
	 * 
	 * @param key 指定的上傳目録
	 * @throws FileNotFoundException
	 */
	public void uploadObject(String key) throws FileNotFoundException {
		AWSCredentials credentials = new BasicAWSCredentials(accessKey,
				secretKey);
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);
		AmazonS3Client conn = new AmazonS3Client(credentials, clientConfig);
		conn.setEndpoint(ENDPOINT);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(file.length());
		metadata.addUserMetadata("managecom", "1");
		metadata.addUserMetadata("name", "zhanglf");
		metadata.addUserMetadata("pro", "hn");

		FileInputStream fi = new FileInputStream(file);

		conn.putObject(bucketName, key, fi, metadata);
		
		GeneratePresignedUrlRequest urlRequest = new GeneratePresignedUrlRequest(bucketName, key);
		URL url = conn.generatePresignedUrl(urlRequest);
		System.out.println(url.toString());
		
	}
	
	
	public void uploadObjectWithClientBuilder(String key) throws FileNotFoundException{
		AWSCredentials credentials=new BasicAWSCredentials(accessKey,secretKey);
		ClientConfiguration clientConfig=new ClientConfiguration();
			clientConfig.setProtocol(Protocol.HTTP);
			AmazonS3ClientBuilder builder=AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials));
			EndpointConfiguration endpointConfiguration=new EndpointConfiguration(ENDPOINT,Regions.AP_NORTHEAST_1.getName());
			builder.setEndpointConfiguration(endpointConfiguration);
			
			AmazonS3 conn=builder.build();
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(file.length());
			metadata.addUserMetadata("managecom", "1");
			metadata.addUserMetadata("name", "zhanglf");
			metadata.addUserMetadata("pro", "hn");

			FileInputStream fi = new FileInputStream(file);

			conn.putObject(bucketName, key, fi, metadata);
			
			GeneratePresignedUrlRequest urlRequest = new GeneratePresignedUrlRequest(bucketName, key);
			URL url = conn.generatePresignedUrl(urlRequest);
			System.out.println(url.toString());
	}
	
	
	public void uploadToS3(String key){
		AmazonS3 s3=new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
		Region usWest2=Region.getRegion(Regions.US_WEST_2);
		s3.setRegion(usWest2);
		s3.setEndpoint(ENDPOINT);
		
		s3.putObject(new PutObjectRequest(bucketName, key, file));
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
		
		GeneratePresignedUrlRequest urlRequest = new GeneratePresignedUrlRequest(bucketName, key);
		Date expirationDate =null;
		try {
			expirationDate=new SimpleDateFormat("yyyy-MM-dd").parse("2017-11-23");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		urlRequest.setExpiration(expirationDate);
		//生成url
		URL url = s3.generatePresignedUrl(urlRequest);
		System.out.println(url.toString());
	}
}

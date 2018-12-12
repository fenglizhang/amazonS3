package com.zhanglf.aws;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

public class AwsCreateBucketDemo extends AwsConfig {
	
	/**
	 * 创建名为bucketName的存储桶。
	 * @param bucketName
	 */
	public void createOneBucket(String bucketName){
		final AmazonS3 s3 = new AmazonS3Client();
		try {
		    Bucket b = s3.createBucket(bucketName);
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
		System.out.println("Done!");
	}
	
	/**
	 * 展示所有的存储桶
	 */
	public void showBucketList(){
		final AmazonS3 s3 = new AmazonS3Client();
		List<Bucket> buckets = s3.listBuckets();
		System.out.println("Your Amazon S3 buckets:");
		for (Bucket b : buckets) {
		    System.out.println("* " + b.getName());
		}
	}
	
	/**
	 * 根据桶的名字删除该桶：具体思路是先删除bucketName中的文件，然后删除BucketName
	 * @param bucketName
	 */
	public void deleteBucketByName(String bucketName){
		final AmazonS3 s3 = new AmazonS3Client();
		try {
		    System.out.println(" - removing objects from bucket");
		    ObjectListing object_listing = s3.listObjects(bucketName);
		    while (true) {
		        for (Iterator<?> iterator =
		                object_listing.getObjectSummaries().iterator();
		                iterator.hasNext();) {
		            S3ObjectSummary summary = (S3ObjectSummary)iterator.next();
		            s3.deleteObject(bucketName, summary.getKey());
		        }

		        // more object_listing to retrieve?
		        if (object_listing.isTruncated()) {
		            object_listing = s3.listNextBatchOfObjects(object_listing);
		        } else {
		            break;
		        }
		    };
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
		
	}
	
	/**
	 * 
	 * @param bucket_name
	 */
	public void deleteBucketVisionList(String bucket_name){
		final AmazonS3 s3 = new AmazonS3Client();
		try {
		    System.out.println(" - removing objects from bucket");
		    ObjectListing object_listing = s3.listObjects(bucket_name);
		    while (true) {
		        for (Iterator<?> iterator =
		                object_listing.getObjectSummaries().iterator();
		                iterator.hasNext();) {
		            S3ObjectSummary summary = (S3ObjectSummary)iterator.next();
		            s3.deleteObject(bucket_name, summary.getKey());
		        }

		        // more object_listing to retrieve?
		        if (object_listing.isTruncated()) {
		            object_listing = s3.listNextBatchOfObjects(object_listing);
		        } else {
		            break;
		        }
		    };

		    System.out.println(" - removing versions from bucket");
		    VersionListing version_listing = s3.listVersions(
		            new ListVersionsRequest().withBucketName(bucket_name));
		    while (true) {
		        for (Iterator<?> iterator =
		                version_listing.getVersionSummaries().iterator();
		                iterator.hasNext();) {
		            S3VersionSummary vs = (S3VersionSummary)iterator.next();
		            s3.deleteVersion(
		                    bucket_name, vs.getKey(), vs.getVersionId());
		        }

		        if (version_listing.isTruncated()) {
		            version_listing = s3.listNextBatchOfVersions(
		                    version_listing);
		        } else {
		            break;
		        }
		    }
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
	}
	/**
	 * 删除空格桶
	 * @param bucket_name
	 */
	public void deleteEmptyBucket(String bucket_name){
		final AmazonS3 s3 = new AmazonS3Client();
		try {
		    s3.deleteBucket(bucket_name);
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
	}
	
}

package com.zhanglf.aws;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AwsOperatorObj {
	// 存储桶的名字
	private String bucket_name = "beijing";
	// 自定义上传到bucket后的目录结构
	private String key_name = "beijing/haidingqu/xierqi/yuanzougaofei.mp3";
	// 本地文件路径
	private String file_path = "E:/music/china/yuanzougaofei.mp3";

	/**
	 * 上传文件到指定的bucket中。 注意点：这里的本地路径的文件必须存在才行，不然文件不存在回异常。
	 */
	public void uploadObj() {
		final AmazonS3 s3 = new AmazonS3Client();
		try {
			s3.putObject(bucket_name, key_name, file_path);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	public void showListObjFromBucket(String bucketName) {
		final AmazonS3 s3 = new AmazonS3Client();
		ObjectListing ol = s3.listObjects(bucketName);

		List<S3ObjectSummary> objects = ol.getObjectSummaries();

		for (S3ObjectSummary os : objects) {
			System.out.println("* " + os.getKey());
		}
	}

	/**
	 * 
	 * @param buketName
	 *            桶名
	 * @param key
	 *            文件在bucket中的路径
	 * 
	 */
	public void downloadObjByKey(String buketName, String key) {
		final AmazonS3 s3 = new AmazonS3Client();
		try {
			S3Object o = s3.getObject(buketName, key);
			S3ObjectInputStream s3is = o.getObjectContent();
			// 下载到指定路径file_path下
			FileOutputStream fos = new FileOutputStream(new File(file_path));
			byte[] read_buf = new byte[1024];
			int read_len = 0;
			while ((read_len = s3is.read(read_buf)) > 0) {
				fos.write(read_buf, 0, read_len);
			}
			s3is.close();
			fos.close();
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	public void moveOrCopyObj(String from_bucket, String object_key,
			String to_bucket, String object_ke) {
		final AmazonS3 s3 = new AmazonS3Client();
		try {
			s3.copyObject(from_bucket, object_key, to_bucket, object_key);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}
	public void deleteObject(String bucket_name, String object_key) {
		final AmazonS3 s3 = new AmazonS3Client();
		try {
			s3.deleteObject(bucket_name, object_key);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}
	/**
	 * 
	 * @param bucket_name
	 * @param object_keys  要删除的obj所在的目录集合
	 */
	public void  deleteObjects(String bucket_name,String[] object_keys){
		final AmazonS3 s3 = new AmazonS3Client();
		try {
		    DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket_name)
		        .withKeys(object_keys);
		    s3.deleteObjects(dor);
		} catch (AmazonServiceException e) {
		    System.err.println(e.getErrorMessage());
		    System.exit(1);
		}
	}

	/**
	 * 刪除bucket目録下所有文件
	 * @param bucketName
	 */
	public void deleteAllObjects(String bucketName) {
		final AmazonS3 s3 = new AmazonS3Client();
		ObjectListing ol = s3.listObjects(bucketName);

		List<S3ObjectSummary> objects = ol.getObjectSummaries();

		for (S3ObjectSummary os : objects) {
			String key = os.getKey();
			s3.deleteObject(bucketName, key);
		}
	}
}

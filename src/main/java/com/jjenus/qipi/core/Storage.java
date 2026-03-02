package com.jjenus.qipi.core;

import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.UploadPartResult;
import com.jjenus.qipi.model.StorageUrl;
import com.jjenus.qipi.exception.StorageException;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Main storage interface providing S3-like functionality with multipart upload support.
 * All operations work consistently across different storage providers.
 */
public interface Storage extends AutoCloseable {
    
    // ==================== Bucket Operations ====================
    
    /**
     * Create a new bucket/container
     */
    void createBucket(String bucketName) throws StorageException;
    
    /**
     * Check if bucket exists
     */
    boolean bucketExists(String bucketName) throws StorageException;
    
    /**
     * Delete an empty bucket
     */
    void deleteBucket(String bucketName) throws StorageException;
    
    /**
     * List all buckets
     */
    List<String> listBuckets() throws StorageException;
    
    // ==================== Basic Object Operations ====================
    
    /**
     * Upload a file/object
     */
    FileInfo putObject(String bucketName, String objectKey, InputStream data, 
                      long contentLength, String contentType, 
                      Map<String, String> metadata) throws StorageException;
    
    /**
     * Upload a file/object from byte array
     */
    FileInfo putObject(String bucketName, String objectKey, byte[] data, 
                      String contentType, Map<String, String> metadata) throws StorageException;
    
    /**
     * Download a file/object
     */
    InputStream getObject(String bucketName, String objectKey) throws StorageException;
    
    /**
     * Download a range of a file/object (for video streaming)
     */
    InputStream getObjectRange(String bucketName, String objectKey, long start, long end) 
            throws StorageException;
    
    /**
     * Delete an object
     */
    void deleteObject(String bucketName, String objectKey) throws StorageException;
    
    /**
     * Check if object exists
     */
    boolean objectExists(String bucketName, String objectKey) throws StorageException;
    
    // ==================== Multipart Upload Operations ====================
    
    /**
     * Initialize a multipart upload
     * @return Upload ID for subsequent operations
     */
    String initiateMultipartUpload(String bucketName, String objectKey, 
                                   String contentType, Map<String, String> metadata) 
            throws StorageException;
    
    /**
     * Upload a part of a multipart upload
     */
    UploadPartResult uploadPart(String bucketName, String objectKey, String uploadId,
                                int partNumber, InputStream data, long partSize) 
            throws StorageException;
    
    /**
     * Complete a multipart upload
     */
    FileInfo completeMultipartUpload(String bucketName, String objectKey, 
                                     String uploadId, List<UploadPartResult> parts) 
            throws StorageException;
    
    /**
     * Abort a multipart upload
     */
    void abortMultipartUpload(String bucketName, String objectKey, String uploadId) 
            throws StorageException;
    
    /**
     * List all active multipart uploads
     */
    List<String> listMultipartUploads(String bucketName) throws StorageException;
    
    // ==================== URL Generation ====================
    
    /**
     * Generate a signed URL for temporary access
     * @param expiryInSeconds URL expiry time in seconds
     */
    StorageUrl generateSignedUrl(String bucketName, String objectKey, 
                                  HttpMethod method, long expiryInSeconds) 
            throws StorageException;
    
    /**
     * Generate a public URL (if bucket allows public access)
     */
    StorageUrl generatePublicUrl(String bucketName, String objectKey) 
            throws StorageException;
    
    /**
     * Generate a signed upload URL for direct browser uploads
     */
    StorageUrl generateUploadUrl(String bucketName, String objectKey, 
                                  String contentType, long expiryInSeconds) 
            throws StorageException;
    
    // ==================== Metadata Operations ====================
    
    /**
     * Get object metadata
     */
    FileInfo getObjectInfo(String bucketName, String objectKey) throws StorageException;
    
    /**
     * Update object metadata
     */
    void updateObjectMetadata(String bucketName, String objectKey, 
                              Map<String, String> metadata) throws StorageException;
    
    /**
     * List objects in a bucket
     */
    List<FileInfo> listObjects(String bucketName) throws StorageException;
    
    /**
     * List objects with prefix
     */
    List<FileInfo> listObjects(String bucketName, String prefix) throws StorageException;
    
    // ==================== Async Operations ====================
    
    /**
     * Async upload with progress tracking
     */
    CompletableFuture<FileInfo> putObjectAsync(String bucketName, String objectKey,
                                               InputStream data, long contentLength,
                                               String contentType, 
                                               Map<String, String> metadata,
                                               ProgressCallback progressCallback);
    
    /**
     * Async download with progress tracking
     */
    CompletableFuture<InputStream> getObjectAsync(String bucketName, String objectKey,
                                                  ProgressCallback progressCallback);
    
    // ==================== Copy and Move ====================
    
    /**
     * Copy object within same storage
     */
    FileInfo copyObject(String sourceBucket, String sourceKey,
                        String destinationBucket, String destinationKey) 
            throws StorageException;
    
    /**
     * Move object within same storage
     */
    FileInfo moveObject(String sourceBucket, String sourceKey,
                        String destinationBucket, String destinationKey) 
            throws StorageException;
    
    // ==================== Batch Operations ====================
    
    /**
     * Delete multiple objects
     */
    void deleteObjects(String bucketName, List<String> objectKeys) throws StorageException;
    
    // ==================== Presigned POST ====================
    
    /**
     * Generate presigned POST data for direct browser upload
     */
    PresignedPost generatePresignedPost(String bucketName, String objectKey,
                                        Map<String, String> conditions,
                                        long expiryInSeconds) throws StorageException;
    
    enum HttpMethod {
        GET, PUT, POST, DELETE
    }
    
    interface ProgressCallback {
        void onProgress(long bytesTransferred, long totalBytes);
    }
    
    class PresignedPost {
        private final String url;
        private final Map<String, String> fields;
        
        public PresignedPost(String url, Map<String, String> fields) {
            this.url = url;
            this.fields = fields;
        }
        
        public String getUrl() { return url; }
        public Map<String, String> getFields() { return fields; }
    }
}

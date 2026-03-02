package com.jjenus.storage.providers;

import com.jjenus.storage.config.StorageConfig;
import com.jjenus.storage.core.Storage;
import com.jjenus.storage.core.StorageException;
import com.jjenus.storage.model.FileInfo;
import com.jjenus.storage.model.StorageUrl;
import com.jjenus.storage.model.UploadPartResult;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class S3StorageProvider implements Storage {
    
    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final StorageConfig config;
    private final Map<String, List<CompletedPart>> multipartParts = new ConcurrentHashMap<>();
    
    public S3StorageProvider(StorageConfig config) {
        this.config = config;
        
        var builder = S3Client.builder();
        var presignerBuilder = S3Presigner.builder();
        
        // Configure endpoint if provided (for MinIO)
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            URI endpointUri = URI.create(
                (config.isUseHttps() ? "https://" : "http://") + 
                config.getEndpoint().replace("http://", "").replace("https://", "")
            );
            builder.endpointOverride(endpointUri);
            presignerBuilder.endpointOverride(endpointUri);
        }
        
        // Configure credentials if provided
        if (config.getAccessKey() != null && !config.getAccessKey().isEmpty()) {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(
                config.getAccessKey(), 
                config.getSecretKey()
            );
            builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
            presignerBuilder.credentialsProvider(StaticCredentialsProvider.create(credentials));
        }
        
        // Configure region
        if (config.getRegion() != null && !config.getRegion().isEmpty()) {
            builder.region(Region.of(config.getRegion()));
            presignerBuilder.region(Region.of(config.getRegion()));
        }
        
        // Configure path style access for MinIO
        builder.serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(config.isPathStyleAccess())
            .build());
        
        this.s3Client = builder.build();
        this.s3Presigner = presignerBuilder.build();
    }
    
    @Override
    public void createBucket(String bucketName) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(fullBucketName)
                .build();
            s3Client.createBucket(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to create bucket: " + bucketName, e);
        }
    }
    
    @Override
    public boolean bucketExists(String bucketName) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            HeadBucketRequest request = HeadBucketRequest.builder()
                .bucket(fullBucketName)
                .build();
            s3Client.headBucket(request);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        } catch (SdkException e) {
            throw new StorageException("Failed to check bucket existence", e);
        }
    }
    
    @Override
    public void deleteBucket(String bucketName) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(fullBucketName)
                .build();
            s3Client.deleteBucket(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to delete bucket: " + bucketName, e);
        }
    }
    
    @Override
    public List<String> listBuckets() throws StorageException {
        try {
            ListBucketsResponse response = s3Client.listBuckets();
            return response.buckets().stream()
                .map(bucket -> stripBucketPrefix(bucket.name()))
                .collect(Collectors.toList());
        } catch (S3Exception e) {
            throw new StorageException("Failed to list buckets", e);
        }
    }
    
    @Override
    public FileInfo putObject(String bucketName, String objectKey, InputStream data,
                              long contentLength, String contentType,
                              Map<String, String> metadata) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .contentType(contentType);
            
            if (metadata != null && !metadata.isEmpty()) {
                requestBuilder.metadata(metadata);
            }
            
            PutObjectResponse response = s3Client.putObject(
                requestBuilder.build(),
                RequestBody.fromInputStream(data, contentLength)
            );
            
            return buildFileInfo(fullBucketName, objectKey, response);
        } catch (S3Exception e) {
            throw new StorageException("Failed to put object: " + objectKey, e);
        }
    }
    
    @Override
    public FileInfo putObject(String bucketName, String objectKey, byte[] data,
                              String contentType, Map<String, String> metadata) 
            throws StorageException {
        try (InputStream is = new java.io.ByteArrayInputStream(data)) {
            return putObject(bucketName, objectKey, is, data.length, contentType, metadata);
        } catch (Exception e) {
            throw new StorageException("Failed to put object: " + objectKey, e);
        }
    }
    
    @Override
    public InputStream getObject(String bucketName, String objectKey) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .build();
            
            return s3Client.getObject(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to get object: " + objectKey, e);
        }
    }
    
    @Override
    public InputStream getObjectRange(String bucketName, String objectKey, long start, long end) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .range("bytes=" + start + "-" + end)
                .build();
            
            return s3Client.getObject(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to get object range: " + objectKey, e);
        }
    }
    
    @Override
    public void deleteObject(String bucketName, String objectKey) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .build();
            s3Client.deleteObject(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to delete object: " + objectKey, e);
        }
    }
    
    @Override
    public boolean objectExists(String bucketName, String objectKey) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            throw new StorageException("Failed to check object existence", e);
        }
    }
    
    @Override
    public String initiateMultipartUpload(String bucketName, String objectKey,
                                          String contentType, Map<String, String> metadata) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .contentType(contentType);
            
            if (metadata != null && !metadata.isEmpty()) {
                requestBuilder.metadata(metadata);
            }
            
            CreateMultipartUploadResponse response = s3Client.createMultipartUpload(
                requestBuilder.build()
            );
            
            return response.uploadId();
        } catch (S3Exception e) {
            throw new StorageException("Failed to initiate multipart upload", e);
        }
    }
    
    @Override
    public UploadPartResult uploadPart(String bucketName, String objectKey, String uploadId,
                                       int partNumber, InputStream data, long partSize) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            UploadPartRequest request = UploadPartRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();
            
            UploadPartResponse response = s3Client.uploadPart(
                request,
                RequestBody.fromInputStream(data, partSize)
            );
            
            return new UploadPartResult(partNumber, response.eTag(), partSize);
        } catch (S3Exception e) {
            throw new StorageException("Failed to upload part " + partNumber, e);
        }
    }
    
    @Override
    public FileInfo completeMultipartUpload(String bucketName, String objectKey,
                                            String uploadId, List<UploadPartResult> parts) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            List<CompletedPart> completedParts = parts.stream()
                .map(part -> CompletedPart.builder()
                    .partNumber(part.getPartNumber())
                    .eTag(part.getEtag())
                    .build())
                .collect(Collectors.toList());
            
            CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build())
                .build();
            
            CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(request);
            
            FileInfo fileInfo = new FileInfo(bucketName, objectKey, 0, Instant.now());
            fileInfo.setEtag(response.eTag());
            fileInfo.setMultipart(true);
            
            return fileInfo;
        } catch (S3Exception e) {
            throw new StorageException("Failed to complete multipart upload", e);
        }
    }
    
    @Override
    public void abortMultipartUpload(String bucketName, String objectKey, String uploadId) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .uploadId(uploadId)
                .build();
            
            s3Client.abortMultipartUpload(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to abort multipart upload", e);
        }
    }
    
    @Override
    public List<String> listMultipartUploads(String bucketName) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            ListMultipartUploadsRequest request = ListMultipartUploadsRequest.builder()
                .bucket(fullBucketName)
                .build();
            
            ListMultipartUploadsResponse response = s3Client.listMultipartUploads(request);
            
            return response.uploads().stream()
                .map(MultipartUpload::uploadId)
                .collect(Collectors.toList());
        } catch (S3Exception e) {
            throw new StorageException("Failed to list multipart uploads", e);
        }
    }
    
    @Override
    public StorageUrl generateSignedUrl(String bucketName, String objectKey,
                                        HttpMethod method, long expiryInSeconds) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            if (method == HttpMethod.GET) {
                GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(fullBucketName)
                    .key(objectKey)
                    .build();
                
                GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .getObjectRequest(getRequest)
                    .signatureDuration(Duration.ofSeconds(expiryInSeconds))
                    .build();
                
                PresignedGetObjectRequest presigned = s3Presigner.presignGetObject(presignRequest);
                
                return new StorageUrl(
                    presigned.url(),
                    Instant.now().plusSeconds(expiryInSeconds),
                    presigned.signedHeaders(),
                    StorageUrl.UrlType.SIGNED
                );
                
            } else if (method == HttpMethod.PUT) {
                PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(fullBucketName)
                    .key(objectKey)
                    .build();
                
                PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .putObjectRequest(putRequest)
                    .signatureDuration(Duration.ofSeconds(expiryInSeconds))
                    .build();
                
                PresignedPutObjectRequest presigned = s3Presigner.presignPutObject(presignRequest);
                
                return new StorageUrl(
                    presigned.url(),
                    Instant.now().plusSeconds(expiryInSeconds),
                    presigned.signedHeaders(),
                    StorageUrl.UrlType.UPLOAD
                );
            }
            
            throw new StorageException("Unsupported HTTP method for signed URL: " + method);
            
        } catch (S3Exception e) {
            throw new StorageException("Failed to generate signed URL", e);
        }
    }
    
    @Override
    public StorageUrl generatePublicUrl(String bucketName, String objectKey) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            // Construct public URL based on endpoint or default AWS URL
            String url;
            if (config.getBaseUrl() != null && !config.getBaseUrl().isEmpty()) {
                url = config.getBaseUrl() + bucketName + "/" + objectKey;
            } else if (config.getEndpoint() != null) {
                String protocol = config.isUseHttps() ? "https://" : "http://";
                url = protocol + config.getEndpoint() + "/" + fullBucketName + "/" + objectKey;
            } else {
                // Default AWS URL format
                url = "https://" + fullBucketName + ".s3." + config.getRegion() + ".amazonaws.com/" + objectKey;
            }
            
            return new StorageUrl(
                new URL(url),
                null,
                Collections.emptyMap(),
                StorageUrl.UrlType.PUBLIC
            );
        } catch (Exception e) {
            throw new StorageException("Failed to generate public URL", e);
        }
    }
    
    @Override
    public StorageUrl generateUploadUrl(String bucketName, String objectKey,
                                        String contentType, long expiryInSeconds) 
            throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .contentType(contentType)
                .build();
            
            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .putObjectRequest(putRequest)
                .signatureDuration(Duration.ofSeconds(expiryInSeconds))
                .build();
            
            PresignedPutObjectRequest presigned = s3Presigner.presignPutObject(presignRequest);
            
            return new StorageUrl(
                presigned.url(),
                Instant.now().plusSeconds(expiryInSeconds),
                presigned.signedHeaders(),
                StorageUrl.UrlType.UPLOAD
            );
        } catch (S3Exception e) {
            throw new StorageException("Failed to generate upload URL", e);
        }
    }
    
    @Override
    public FileInfo getObjectInfo(String bucketName, String objectKey) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(fullBucketName)
                .key(objectKey)
                .build();
            
            HeadObjectResponse response = s3Client.headObject(request);
            
            return buildFileInfo(fullBucketName, objectKey, response);
        } catch (S3Exception e) {
            throw new StorageException("Failed to get object info", e);
        }
    }
    
    @Override
    public void updateObjectMetadata(String bucketName, String objectKey,
                                     Map<String, String> metadata) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            // First copy the object to itself with new metadata
            CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(fullBucketName)
                .sourceKey(objectKey)
                .destinationBucket(fullBucketName)
                .destinationKey(objectKey)
                .metadata(metadata)
                .metadataDirective(MetadataDirective.REPLACE)
                .build();
            
            s3Client.copyObject(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to update metadata", e);
        }
    }
    
    @Override
    public List<FileInfo> listObjects(String bucketName) throws StorageException {
        return listObjects(bucketName, "");
    }
    
    @Override
    public List<FileInfo> listObjects(String bucketName, String prefix) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(fullBucketName)
                .prefix(prefix)
                .build();
            
            ListObjectsV2Iterable responses = s3Client.listObjectsV2Paginator(request);
            
            List<FileInfo> objects = new ArrayList<>();
            for (ListObjectsV2Response response : responses) {
                for (S3Object s3Object : response.contents()) {
                    FileInfo fileInfo = new FileInfo(
                        bucketName,
                        s3Object.key(),
                        s3Object.size(),
                        s3Object.lastModified()
                    );
                    fileInfo.setEtag(s3Object.eTag());
                    fileInfo.setStorageClass(FileInfo.StorageClass.valueOf(s3Object.storageClassAsString()));
                    objects.add(fileInfo);
                }
            }
            
            return objects;
        } catch (S3Exception e) {
            throw new StorageException("Failed to list objects", e);
        }
    }
    
    @Override
    public CompletableFuture<FileInfo> putObjectAsync(String bucketName, String objectKey,
                                                       InputStream data, long contentLength,
                                                       String contentType,
                                                       Map<String, String> metadata,
                                                       ProgressCallback progressCallback) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // In a real implementation, you'd use a custom async client
                return putObject(bucketName, objectKey, data, contentLength, contentType, metadata);
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    @Override
    public CompletableFuture<InputStream> getObjectAsync(String bucketName, String objectKey,
                                                          ProgressCallback progressCallback) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return getObject(bucketName, objectKey);
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    @Override
    public FileInfo copyObject(String sourceBucket, String sourceKey,
                                String destinationBucket, String destinationKey) 
            throws StorageException {
        try {
            String fullSourceBucket = getFullBucketName(sourceBucket);
            String fullDestBucket = getFullBucketName(destinationBucket);
            
            CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(fullSourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(fullDestBucket)
                .destinationKey(destinationKey)
                .build();
            
            CopyObjectResponse response = s3Client.copyObject(request);
            
            return new FileInfo(destinationBucket, destinationKey, 0, Instant.now());
        } catch (S3Exception e) {
            throw new StorageException("Failed to copy object", e);
        }
    }
    
    @Override
    public FileInfo moveObject(String sourceBucket, String sourceKey,
                                String destinationBucket, String destinationKey) 
            throws StorageException {
        try {
            // Copy first
            FileInfo fileInfo = copyObject(sourceBucket, sourceKey, destinationBucket, destinationKey);
            
            // Then delete source
            deleteObject(sourceBucket, sourceKey);
            
            return fileInfo;
        } catch (StorageException e) {
            throw new StorageException("Failed to move object", e);
        }
    }
    
    @Override
    public void deleteObjects(String bucketName, List<String> objectKeys) throws StorageException {
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            List<ObjectIdentifier> objects = objectKeys.stream()
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());
            
            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(fullBucketName)
                .delete(Delete.builder().objects(objects).build())
                .build();
            
            s3Client.deleteObjects(request);
        } catch (S3Exception e) {
            throw new StorageException("Failed to delete multiple objects", e);
        }
    }
    
    @Override
    public PresignedPost generatePresignedPost(String bucketName, String objectKey,
                                                Map<String, String> conditions,
                                                long expiryInSeconds) throws StorageException {
        // Note: AWS SDK v2 doesn't have direct POST policy generation in presigner
        // This is a simplified version
        try {
            String fullBucketName = getFullBucketName(bucketName);
            
            Map<String, String> fields = new HashMap<>();
            fields.put("key", objectKey);
            fields.put("bucket", fullBucketName);
            
            if (conditions != null) {
                fields.putAll(conditions);
            }
            
            String url;
            if (config.getEndpoint() != null) {
                url = (config.isUseHttps() ? "https://" : "http://") + 
                      config.getEndpoint() + "/" + fullBucketName;
            } else {
                url = "https://" + fullBucketName + ".s3.amazonaws.com/";
            }
            
            return new PresignedPost(url, fields);
        } catch (Exception e) {
            throw new StorageException("Failed to generate presigned POST", e);
        }
    }
    
    @Override
    public void close() throws Exception {
        if (s3Client != null) {
            s3Client.close();
        }
        if (s3Presigner != null) {
            s3Presigner.close();
        }
    }
    
    // Helper methods
    private String getFullBucketName(String bucketName) {
        if (config.getBucketPrefix() != null && !config.getBucketPrefix().isEmpty()) {
            return config.getBucketPrefix() + "-" + bucketName;
        }
        return bucketName;
    }
    
    private String stripBucketPrefix(String fullBucketName) {
        if (config.getBucketPrefix() != null && !config.getBucketPrefix().isEmpty()) {
            String prefix = config.getBucketPrefix() + "-";
            if (fullBucketName.startsWith(prefix)) {
                return fullBucketName.substring(prefix.length());
            }
        }
        return fullBucketName;
    }
    
    private FileInfo buildFileInfo(String bucketName, String objectKey, 
                                    PutObjectResponse response) {
        FileInfo fileInfo = new FileInfo(
            stripBucketPrefix(bucketName),
            objectKey,
            0, // Size not available in PutObjectResponse
            Instant.now()
        );
        fileInfo.setEtag(response.eTag());
        if (response.versionId() != null) {
            fileInfo.setVersionId(response.versionId());
        }
        return fileInfo;
    }
    
    private FileInfo buildFileInfo(String bucketName, String objectKey,
                                    HeadObjectResponse response) {
        FileInfo fileInfo = new FileInfo(
            stripBucketPrefix(bucketName),
            objectKey,
            response.contentLength(),
            response.lastModified()
        );
        fileInfo.setEtag(response.eTag());
        fileInfo.setContentType(response.contentType());
        fileInfo.setUserMetadata(response.metadata());
        return fileInfo;
    }
}

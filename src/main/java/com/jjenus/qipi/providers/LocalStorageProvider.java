package com.jjenus.qipi.providers;

import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.exception.StorageException;
import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.StorageUrl;
import com.jjenus.qipi.model.UploadPartResult;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LocalStorageProvider implements Storage {
    
    private final Path rootPath;
    private final StorageConfig config;
    private final Map<String, MultipartUpload> activeMultipartUploads = new ConcurrentHashMap<>();
    
    private static class MultipartUpload {
        final String uploadId;
        final String bucketName;
        final String objectKey;
        final String contentType;
        final Map<String, String> metadata;
        final List<UploadPartResult> parts = new ArrayList<>();
        final Path tempDir;
        final Instant createdAt;
        
        MultipartUpload(String uploadId, String bucketName, String objectKey,
                       String contentType, Map<String, String> metadata, Path tempDir) {
            this.uploadId = uploadId;
            this.bucketName = bucketName;
            this.objectKey = objectKey;
            this.contentType = contentType;
            this.metadata = metadata;
            this.tempDir = tempDir;
            this.createdAt = Instant.now();
        }
    }
    
    public LocalStorageProvider(StorageConfig config) throws IOException {
        this.config = config;
        this.rootPath = Paths.get(config.getBasePath()).toAbsolutePath();
        Files.createDirectories(rootPath);
    }
    
    private Path getBucketPath(String bucketName) throws StorageException {
        Path bucketPath = rootPath.resolve(bucketName);
        if (!Files.exists(bucketPath)) {
            throw new StorageException(StorageException.ErrorCode.BUCKET_NOT_FOUND,
                "Bucket does not exist: " + bucketName, bucketName, null);
        }
        return bucketPath;
    }
    
    private Path getObjectPath(String bucketName, String objectKey) throws StorageException {
        Path bucketPath = getBucketPath(bucketName);
        
        // Sanitize object key to prevent path traversal
        String sanitizedKey = objectKey.replace("..", "").replace("/", File.separator);
        Path objectPath = bucketPath.resolve(sanitizedKey);
        
        // Ensure the resolved path is still within the bucket
        try {
            if (!objectPath.toRealPath().startsWith(bucketPath.toRealPath())) {
                throw new StorageException(StorageException.ErrorCode.INVALID_REQUEST,
                    "Invalid object key: path traversal detected", bucketName, objectKey);
            }
        } catch (IOException e) {
            throw new StorageException("Error resolving path", e);
        }
        
        return objectPath;
    }
    
    @Override
    public void createBucket(String bucketName) throws StorageException {
        try {
            Path bucketPath = rootPath.resolve(bucketName);
            Files.createDirectories(bucketPath);
        } catch (IOException e) {
            throw new StorageException("Failed to create bucket: " + bucketName, e);
        }
    }
    
    @Override
    public boolean bucketExists(String bucketName) throws StorageException {
        Path bucketPath = rootPath.resolve(bucketName);
        return Files.exists(bucketPath) && Files.isDirectory(bucketPath);
    }
    
    @Override
    public void deleteBucket(String bucketName) throws StorageException {
        try {
            Path bucketPath = rootPath.resolve(bucketName);
            if (!Files.exists(bucketPath)) {
                throw new StorageException(StorageException.ErrorCode.BUCKET_NOT_FOUND,
                    "Bucket not found: " + bucketName, bucketName, null);
            }
            
            // Check if bucket is empty
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(bucketPath)) {
                if (stream.iterator().hasNext()) {
                    throw new StorageException(StorageException.ErrorCode.BUCKET_NOT_EMPTY,
                        "Cannot delete non-empty bucket: " + bucketName, bucketName, null);
                }
            }
            
            Files.delete(bucketPath);
        } catch (IOException e) {
            throw new StorageException("Failed to delete bucket: " + bucketName, e);
        }
    }
    
    @Override
    public List<String> listBuckets() throws StorageException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
            List<String> buckets = new ArrayList<>();
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    buckets.add(path.getFileName().toString());
                }
            }
            return buckets;
        } catch (IOException e) {
            throw new StorageException("Failed to list buckets", e);
        }
    }
    
    @Override
    public FileInfo putObject(String bucketName, String objectKey, InputStream data,
                              long contentLength, String contentType,
                              Map<String, String> metadata) throws StorageException {
        try {
            Path bucketPath = rootPath.resolve(bucketName);
            if (!Files.exists(bucketPath)) {
                createBucket(bucketName);
            }
            
            Path objectPath = bucketPath.resolve(objectKey);
            Files.createDirectories(objectPath.getParent());
            
            // Calculate ETag (MD5 of content)
            MessageDigest md = MessageDigest.getInstance("MD5");
            
            try (OutputStream os = Files.newOutputStream(objectPath, StandardOpenOption.CREATE)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                long totalBytes = 0;
                
                while ((bytesRead = data.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                    md.update(buffer, 0, bytesRead);
                    totalBytes += bytesRead;
                }
            }
            
            byte[] digest = md.digest();
            String etag = bytesToHex(digest);
            
            FileInfo fileInfo = new FileInfo(bucketName, objectKey, contentLength, Instant.now());
            fileInfo.setEtag(etag);
            fileInfo.setContentType(contentType);
            fileInfo.setUserMetadata(metadata != null ? metadata : new HashMap<>());
            
            return fileInfo;
        } catch (Exception e) {
            throw new StorageException("Failed to put object: " + objectKey, e);
        }
    }
    
    @Override
    public FileInfo putObject(String bucketName, String objectKey, byte[] data,
                              String contentType, Map<String, String> metadata) 
            throws StorageException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
            return putObject(bucketName, objectKey, bais, data.length, contentType, metadata);
        }
    }
    
    @Override
    public InputStream getObject(String bucketName, String objectKey) throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            
            if (!Files.exists(objectPath)) {
                throw new StorageException(StorageException.ErrorCode.OBJECT_NOT_FOUND,
                    "Object does not exist: " + objectKey, bucketName, objectKey);
            }
            
            return Files.newInputStream(objectPath);
        } catch (IOException e) {
            throw new StorageException("Failed to get object: " + objectKey, e);
        }
    }
    
    @Override
    public InputStream getObjectRange(String bucketName, String objectKey, long start, long end) 
            throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            
            if (!Files.exists(objectPath)) {
                throw new StorageException(StorageException.ErrorCode.OBJECT_NOT_FOUND,
                    "Object does not exist: " + objectKey, bucketName, objectKey);
            }
            
            RandomAccessFile raf = new RandomAccessFile(objectPath.toFile(), "r");
            raf.seek(start);
            
            // Create a limited input stream for the range
            long length = end - start + 1;
            byte[] buffer = new byte[(int)length];
            raf.read(buffer);
            raf.close();
            
            return new ByteArrayInputStream(buffer);
        } catch (IOException e) {
            throw new StorageException("Failed to get object range: " + objectKey, e);
        }
    }
    
    @Override
    public void deleteObject(String bucketName, String objectKey) throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            Files.deleteIfExists(objectPath);
            
            // Clean up empty parent directories
            Path parent = objectPath.getParent();
            Path bucketPath = rootPath.resolve(bucketName);
            
            while (parent != null && !parent.equals(bucketPath)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
                    if (!stream.iterator().hasNext()) {
                        Files.delete(parent);
                    }
                }
                parent = parent.getParent();
            }
        } catch (IOException e) {
            throw new StorageException("Failed to delete object: " + objectKey, e);
        }
    }
    
    @Override
    public boolean objectExists(String bucketName, String objectKey) throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            return Files.exists(objectPath);
        } catch (StorageException e) {
            return false;
        }
    }
    
    @Override
    public String initiateMultipartUpload(String bucketName, String objectKey,
                                          String contentType, Map<String, String> metadata) 
            throws StorageException {
        try {
            String uploadId = UUID.randomUUID().toString();
            Path tempDir = Files.createTempDirectory("multipart_" + uploadId);
            
            MultipartUpload upload = new MultipartUpload(
                uploadId, bucketName, objectKey, contentType, metadata, tempDir
            );
            
            activeMultipartUploads.put(uploadId, upload);
            return uploadId;
        } catch (IOException e) {
            throw new StorageException("Failed to initiate multipart upload", e);
        }
    }
    
    @Override
    public UploadPartResult uploadPart(String bucketName, String objectKey, String uploadId,
                                       int partNumber, InputStream data, long partSize) 
            throws StorageException {
        MultipartUpload upload = activeMultipartUploads.get(uploadId);
        if (upload == null) {
            throw new StorageException("Invalid upload ID: " + uploadId);
        }
        
        try {
            Path partFile = upload.tempDir.resolve("part_" + partNumber);
            
            // Calculate ETag for this part
            MessageDigest md = MessageDigest.getInstance("MD5");
            
            try (OutputStream os = Files.newOutputStream(partFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                
                while ((bytesRead = data.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                    md.update(buffer, 0, bytesRead);
                }
            }
            
            byte[] digest = md.digest();
            String etag = bytesToHex(digest);
            
            UploadPartResult result = new UploadPartResult(partNumber, etag, partSize);
            upload.parts.add(result);
            
            return result;
        } catch (Exception e) {
            throw new StorageException("Failed to upload part " + partNumber, e);
        }
    }
    
    @Override
    public FileInfo completeMultipartUpload(String bucketName, String objectKey,
                                            String uploadId, List<UploadPartResult> parts) 
            throws StorageException {
        MultipartUpload upload = activeMultipartUploads.remove(uploadId);
        if (upload == null) {
            throw new StorageException("Invalid upload ID: " + uploadId);
        }
        
        try {
            // Sort parts by number
            upload.parts.sort(Comparator.comparingInt(UploadPartResult::getPartNumber));
            
            // Combine parts into final file
            Path finalPath = getObjectPath(bucketName, objectKey);
            Files.createDirectories(finalPath.getParent());
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            long totalSize = 0;
            
            try (OutputStream os = Files.newOutputStream(finalPath)) {
                for (UploadPartResult part : upload.parts) {
                    Path partFile = upload.tempDir.resolve("part_" + part.getPartNumber());
                    byte[] partData = Files.readAllBytes(partFile);
                    os.write(partData);
                    md.update(partData);
                    totalSize += partData.length;
                }
            }
            
            // Clean up temp directory
            deleteDirectory(upload.tempDir);
            
            byte[] digest = md.digest();
            String etag = bytesToHex(digest);
            
            FileInfo fileInfo = new FileInfo(bucketName, objectKey, totalSize, Instant.now());
            fileInfo.setEtag(etag);
            fileInfo.setContentType(upload.contentType);
            fileInfo.setUserMetadata(upload.metadata);
            fileInfo.setMultipart(true);
            
            return fileInfo;
        } catch (Exception e) {
            throw new StorageException("Failed to complete multipart upload", e);
        }
    }
    
    @Override
    public void abortMultipartUpload(String bucketName, String objectKey, String uploadId) 
            throws StorageException {
        MultipartUpload upload = activeMultipartUploads.remove(uploadId);
        if (upload != null) {
            try {
                deleteDirectory(upload.tempDir);
            } catch (IOException e) {
                throw new StorageException("Failed to abort multipart upload", e);
            }
        }
    }
    
    @Override
    public List<String> listMultipartUploads(String bucketName) throws StorageException {
        return activeMultipartUploads.values().stream()
            .filter(u -> u.bucketName.equals(bucketName))
            .map(u -> u.uploadId)
            .collect(Collectors.toList());
    }
    
    @Override
    public StorageUrl generateSignedUrl(String bucketName, String objectKey,
                                        HttpMethod method, long expiryInSeconds) 
            throws StorageException {
        try {
            // For local storage, we generate a signed URL with a token
            String baseUrl = config.getBaseUrl();
            if (baseUrl == null) {
                baseUrl = "http://localhost:8080/files/";
            }
            
            String signingKey = config.getSigningKey();
            if (signingKey == null) {
                signingKey = "default-signing-key";
            }
            
            long expiryTime = Instant.now().plusSeconds(expiryInSeconds).toEpochMilli();
            
            // Create a signature
            String dataToSign = bucketName + ":" + objectKey + ":" + expiryTime + ":" + method;
            String signature = hmacSha256(signingKey, dataToSign);
            
            String url = baseUrl + bucketName + "/" + objectKey + 
                        "?expiry=" + expiryTime + "&signature=" + signature;
            
            return new StorageUrl(
                new URL(url),
                Instant.now().plusSeconds(expiryInSeconds),
                Collections.emptyMap(),
                method == HttpMethod.PUT ? StorageUrl.UrlType.UPLOAD : StorageUrl.UrlType.SIGNED
            );
        } catch (Exception e) {
            throw new StorageException("Failed to generate signed URL", e);
        }
    }
    
    @Override
    public StorageUrl generatePublicUrl(String bucketName, String objectKey) throws StorageException {
        try {
            String baseUrl = config.getBaseUrl();
            if (baseUrl == null) {
                baseUrl = "http://localhost:8080/files/";
            }
            
            String url = baseUrl + bucketName + "/" + objectKey;
            return new StorageUrl(new URL(url), null, Collections.emptyMap(), StorageUrl.UrlType.PUBLIC);
        } catch (Exception e) {
            throw new StorageException("Failed to generate public URL", e);
        }
    }
    
    @Override
    public StorageUrl generateUploadUrl(String bucketName, String objectKey,
                                        String contentType, long expiryInSeconds) 
            throws StorageException {
        return generateSignedUrl(bucketName, objectKey, HttpMethod.PUT, expiryInSeconds);
    }
    
    @Override
    public FileInfo getObjectInfo(String bucketName, String objectKey) throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            BasicFileAttributes attrs = Files.readAttributes(objectPath, BasicFileAttributes.class);
            
            FileInfo fileInfo = new FileInfo(
                bucketName,
                objectKey,
                attrs.size(),
                attrs.lastModifiedTime().toInstant()
            );
            
            // Try to read metadata from a sidecar file
            Path metaPath = objectPath.resolveSibling(objectPath.getFileName() + ".meta");
            if (Files.exists(metaPath)) {
                Properties props = new Properties();
                try (InputStream is = Files.newInputStream(metaPath)) {
                    props.load(is);
                    fileInfo.setContentType(props.getProperty("content-type", "application/octet-stream"));
                    
                    Map<String, String> metadata = new HashMap<>();
                    props.stringPropertyNames().stream()
                        .filter(k -> !k.equals("content-type"))
                        .forEach(k -> metadata.put(k, props.getProperty(k)));
                    fileInfo.setUserMetadata(metadata);
                }
            }
            
            return fileInfo;
        } catch (IOException e) {
            throw new StorageException("Failed to get object info", e);
        }
    }
    
    @Override
    public void updateObjectMetadata(String bucketName, String objectKey,
                                     Map<String, String> metadata) throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            Path metaPath = objectPath.resolveSibling(objectPath.getFileName() + ".meta");
            
            Properties props = new Properties();
            
            // Load existing metadata if any
            if (Files.exists(metaPath)) {
                try (InputStream is = Files.newInputStream(metaPath)) {
                    props.load(is);
                }
            }
            
            // Update with new metadata
            if (metadata != null) {
                props.putAll(metadata);
            }
            
            // Save back
            try (OutputStream os = Files.newOutputStream(metaPath)) {
                props.store(os, "File metadata");
            }
        } catch (IOException e) {
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
            Path bucketPath = getBucketPath(bucketName);
            Path searchPath = bucketPath.resolve(prefix);
            
            List<FileInfo> objects = new ArrayList<>();
            
            if (Files.exists(searchPath)) {
                Files.walkFileTree(searchPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        // Skip metadata files
                        if (file.toString().endsWith(".meta")) {
                            return FileVisitResult.CONTINUE;
                        }
                        
                        String relativePath = bucketPath.relativize(file).toString();
                        objects.add(new FileInfo(
                            bucketName,
                            relativePath,
                            attrs.size(),
                            attrs.lastModifiedTime().toInstant()
                        ));
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            
            return objects;
        } catch (IOException e) {
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
            Path sourcePath = getObjectPath(sourceBucket, sourceKey);
            Path destPath = getObjectPath(destinationBucket, destinationKey);
            
            Files.createDirectories(destPath.getParent());
            Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
            
            // Also copy metadata if exists
            Path sourceMeta = sourcePath.resolveSibling(sourcePath.getFileName() + ".meta");
            if (Files.exists(sourceMeta)) {
                Path destMeta = destPath.resolveSibling(destPath.getFileName() + ".meta");
                Files.copy(sourceMeta, destMeta, StandardCopyOption.REPLACE_EXISTING);
            }
            
            return getObjectInfo(destinationBucket, destinationKey);
        } catch (IOException e) {
            throw new StorageException("Failed to copy object", e);
        }
    }
    
    @Override
    public FileInfo moveObject(String sourceBucket, String sourceKey,
                                String destinationBucket, String destinationKey) 
            throws StorageException {
        try {
            Path sourcePath = getObjectPath(sourceBucket, sourceKey);
            Path destPath = getObjectPath(destinationBucket, destinationKey);
            
            Files.createDirectories(destPath.getParent());
            Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
            
            // Also move metadata if exists
            Path sourceMeta = sourcePath.resolveSibling(sourcePath.getFileName() + ".meta");
            if (Files.exists(sourceMeta)) {
                Path destMeta = destPath.resolveSibling(destPath.getFileName() + ".meta");
                Files.move(sourceMeta, destMeta, StandardCopyOption.REPLACE_EXISTING);
            }
            
            return getObjectInfo(destinationBucket, destinationKey);
        } catch (IOException e) {
            throw new StorageException("Failed to move object", e);
        }
    }
    
    @Override
    public void deleteObjects(String bucketName, List<String> objectKeys) throws StorageException {
        for (String key : objectKeys) {
            deleteObject(bucketName, key);
        }
    }
    
    @Override
    public PresignedPost generatePresignedPost(String bucketName, String objectKey,
                                                Map<String, String> conditions,
                                                long expiryInSeconds) throws StorageException {
        String baseUrl = config.getBaseUrl();
        if (baseUrl == null) {
            baseUrl = "http://localhost:8080/upload/";
        }
        
        Map<String, String> fields = new HashMap<>();
        fields.put("key", objectKey);
        fields.put("bucket", bucketName);
        
        if (conditions != null) {
            fields.putAll(conditions);
        }
        
        return new PresignedPost(baseUrl, fields);
    }
    
    @Override
    public void close() throws Exception {
        // Nothing to close for local storage
    }
    
    // Helper methods
    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
    
    private String hmacSha256(String key, String data) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        javax.crypto.spec.SecretKeySpec secretKey = new javax.crypto.spec.SecretKeySpec(
            key.getBytes("UTF-8"), "HmacSHA256"
        );
        mac.init(secretKey);
        byte[] hmacBytes = mac.doFinal(data.getBytes("UTF-8"));
        return bytesToHex(hmacBytes);
    }
    
    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
                
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}

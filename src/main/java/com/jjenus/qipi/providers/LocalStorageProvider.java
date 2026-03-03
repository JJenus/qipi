package com.jjenus.qipi.providers;

import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.exception.StorageException;
import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.StorageUrl;
import com.jjenus.qipi.model.UploadPartResult;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LocalStorageProvider implements Storage {
    
    private final Path rootPath;
    private final StorageConfig config;
    private final Map<String, MultipartUpload> activeMultipartUploads = new ConcurrentHashMap<>();
    private final Thread cleanupThread;
    private volatile boolean running = true;
    
    private static class MultipartUpload {
        final String uploadId;
        final String bucketName;
        final String objectKey;
        final String contentType;
        final Map<String, String> metadata;
        final List<UploadPartResult> parts = Collections.synchronizedList(new ArrayList<>());
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
    
    public LocalStorageProvider(StorageConfig config) throws StorageException {
        this.config = config;
        try {
            this.rootPath = Paths.get(config.getBasePath()).toAbsolutePath().normalize();
            Files.createDirectories(rootPath);
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to initialize local storage at path: " + config.getBasePath(), e);
        }
        
        // Start cleanup thread for stale multipart uploads
        this.cleanupThread = new Thread(this::cleanupTask);
        this.cleanupThread.setDaemon(true);
        this.cleanupThread.start();
    }
    
    private void cleanupTask() {
        while (running) {
            try {
                Thread.sleep(3600000); // 1 hour
                cleanupStaleMultipartUploads();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private void cleanupStaleMultipartUploads() {
        Instant cutoff = Instant.now().minusSeconds(86400); // 24 hours
        List<String> staleUploads = activeMultipartUploads.entrySet().stream()
            .filter(entry -> entry.getValue().createdAt.isBefore(cutoff))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        
        for (String uploadId : staleUploads) {
            MultipartUpload upload = activeMultipartUploads.remove(uploadId);
            if (upload != null) {
                try {
                    deleteDirectory(upload.tempDir);
                } catch (IOException e) {
                    // Log but continue - use System.err for now, consider adding proper logging
                    System.err.println("Failed to delete stale multipart upload directory: " + e.getMessage());
                }
            }
        }
    }
    
    private Path getBucketPath(String bucketName) throws StorageException {
        Path bucketPath = rootPath.resolve(bucketName).normalize();
        
        // Security check - ensure path is within root
        if (!bucketPath.startsWith(rootPath)) {
            throw new StorageException(StorageException.ErrorCode.ACCESS_DENIED,
                "Invalid bucket path", bucketName, null);
        }
        
        if (!Files.exists(bucketPath)) {
            throw new StorageException(StorageException.ErrorCode.BUCKET_NOT_FOUND,
                "Bucket does not exist: " + bucketName, bucketName, null);
        }
        return bucketPath;
    }
    
    private Path getObjectPath(String bucketName, String objectKey) throws StorageException {
        Path bucketPath = rootPath.resolve(bucketName).normalize();
        
        // Security check - ensure bucket path is within root
        if (!bucketPath.startsWith(rootPath)) {
            throw new StorageException(StorageException.ErrorCode.ACCESS_DENIED,
                "Invalid bucket path", bucketName, null);
        }
        
        // Normalize and resolve object key
        Path objectPath = bucketPath.resolve(objectKey).normalize();
        
        // Ensure the resolved path is still within the bucket
        if (!objectPath.startsWith(bucketPath)) {
            throw new StorageException(StorageException.ErrorCode.INVALID_REQUEST,
                "Invalid object key: path traversal detected", bucketName, objectKey);
        }
        
        return objectPath;
    }
    
    @Override
    public void createBucket(String bucketName) throws StorageException {
        try {
            Path bucketPath = rootPath.resolve(bucketName).normalize();
            
            // Security check
            if (!bucketPath.startsWith(rootPath)) {
                throw new StorageException(StorageException.ErrorCode.ACCESS_DENIED,
                    "Invalid bucket name", bucketName, null);
            }
            
            Files.createDirectories(bucketPath);
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to create bucket: " + bucketName, e);
        }
    }
    
    @Override
    public boolean bucketExists(String bucketName) throws StorageException {
        Path bucketPath = rootPath.resolve(bucketName).normalize();
        
        // Security check
        if (!bucketPath.startsWith(rootPath)) {
            return false;
        }
        
        return Files.exists(bucketPath) && Files.isDirectory(bucketPath);
    }
    
    @Override
    public void deleteBucket(String bucketName) throws StorageException {
        try {
            Path bucketPath = getBucketPath(bucketName);
            
            // Check if bucket is empty
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(bucketPath)) {
                if (stream.iterator().hasNext()) {
                    throw new StorageException(StorageException.ErrorCode.BUCKET_NOT_EMPTY,
                        "Cannot delete non-empty bucket: " + bucketName, bucketName, null);
                }
            }
            
            Files.delete(bucketPath);
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to delete bucket: " + bucketName, e);
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
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to list buckets", e);
        }
    }
    
    @Override
    public FileInfo putObject(String bucketName, String objectKey, InputStream data,
                              long contentLength, String contentType,
                              Map<String, String> metadata) throws StorageException {
        Path objectPath = null;
        try {
            Path bucketPath = rootPath.resolve(bucketName).normalize();
            if (!Files.exists(bucketPath)) {
                createBucket(bucketName);
            }
            
            objectPath = getObjectPath(bucketName, objectKey);
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
            
            // Save metadata
            if (metadata != null || contentType != null) {
                saveMetadata(objectPath, contentType, metadata);
            }
            
            FileInfo fileInfo = new FileInfo(bucketName, objectKey, contentLength, Instant.now());
            fileInfo.setEtag(etag);
            fileInfo.setContentType(contentType);
            fileInfo.setUserMetadata(metadata != null ? metadata : new HashMap<>());
            
            return fileInfo;
            
        } catch (NoSuchAlgorithmException e) {
            // This shouldn't happen as MD5 is always available
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "MD5 algorithm not available", e);
        } catch (IOException e) {
            // Clean up partial file if upload failed
            if (objectPath != null) {
                try {
                    Files.deleteIfExists(objectPath);
                } catch (IOException ex) {
                    // Ignore cleanup errors
                }
            }
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to put object: " + objectKey, e);
        }
    }
    
    private void saveMetadata(Path objectPath, String contentType, Map<String, String> metadata) 
            throws IOException {
        Path metaPath = objectPath.resolveSibling(objectPath.getFileName() + ".meta");
        Properties props = new Properties();
        
        if (contentType != null) {
            props.setProperty("content-type", contentType);
        }
        
        if (metadata != null) {
            props.putAll(metadata);
        }
        
        try (OutputStream os = Files.newOutputStream(metaPath, StandardOpenOption.CREATE)) {
            props.store(os, "File metadata");
        }
    }
    
    @Override
    public FileInfo putObject(String bucketName, String objectKey, byte[] data,
                              String contentType, Map<String, String> metadata) 
            throws StorageException {
        // Using ByteArrayInputStream without try-with-resources since it doesn't need closing
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        return putObject(bucketName, objectKey, bais, data.length, contentType, metadata);
        // No need to close ByteArrayInputStream - it's a no-op
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
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to get object: " + objectKey, e);
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
            
            // Return channel as InputStream for efficient streaming
            return new InputStream() {
                private final FileChannel channel = raf.getChannel();
                private long position = start;
                private final long endPosition = end;
                
                @Override
                public int read() throws IOException {
                    if (position > endPosition) {
                        return -1;
                    }
                    byte[] b = new byte[1];
                    int read = channel.read(java.nio.ByteBuffer.wrap(b), position);
                    if (read == 1) {
                        position++;
                        return b[0] & 0xFF;
                    }
                    return -1;
                }
                
                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (position > endPosition) {
                        return -1;
                    }
                    long remaining = endPosition - position + 1;
                    int bytesToRead = (int) Math.min(len, remaining);
                    if (bytesToRead <= 0) {
                        return -1;
                    }
                    java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b, off, bytesToRead);
                    int read = channel.read(buf, position);
                    if (read > 0) {
                        position += read;
                    }
                    return read;
                }
                
                @Override
                public void close() throws IOException {
                    channel.close();
                    raf.close();
                }
            };
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to get object range: " + objectKey, e);
        }
    }
    
    @Override
    public void deleteObject(String bucketName, String objectKey) throws StorageException {
        try {
            Path objectPath = getObjectPath(bucketName, objectKey);
            Files.deleteIfExists(objectPath);
            
            // Delete metadata if exists
            Path metaPath = objectPath.resolveSibling(objectPath.getFileName() + ".meta");
            Files.deleteIfExists(metaPath);
            
            // Clean up empty parent directories
            Path parent = objectPath.getParent();
            Path bucketPath = rootPath.resolve(bucketName).normalize();
            
            while (parent != null && !parent.equals(bucketPath)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
                    if (!stream.iterator().hasNext()) {
                        Files.delete(parent);
                        parent = parent.getParent();
                    } else {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to delete object: " + objectKey, e);
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
            throw new StorageException(StorageException.ErrorCode.MULTIPART_UPLOAD_ERROR,
                "Failed to initiate multipart upload", e);
        }
    }
    
    @Override
    public UploadPartResult uploadPart(String bucketName, String objectKey, String uploadId,
                                       int partNumber, InputStream data, long partSize) 
            throws StorageException {
        MultipartUpload upload = activeMultipartUploads.get(uploadId);
        if (upload == null) {
            throw new StorageException(StorageException.ErrorCode.INVALID_REQUEST,
                "Invalid upload ID: " + uploadId, bucketName, objectKey);
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
        } catch (NoSuchAlgorithmException e) {
            throw new StorageException(StorageException.ErrorCode.MULTIPART_UPLOAD_ERROR,
                "MD5 algorithm not available", e);
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.MULTIPART_UPLOAD_ERROR,
                "Failed to upload part " + partNumber, e);
        }
    }
    
    @Override
    public FileInfo completeMultipartUpload(String bucketName, String objectKey,
                                            String uploadId, List<UploadPartResult> parts) 
            throws StorageException {
        MultipartUpload upload = activeMultipartUploads.remove(uploadId);
        if (upload == null) {
            throw new StorageException(StorageException.ErrorCode.INVALID_REQUEST,
                "Invalid upload ID: " + uploadId, bucketName, objectKey);
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
            
            // Save metadata
            saveMetadata(finalPath, upload.contentType, upload.metadata);
            
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
        } catch (NoSuchAlgorithmException e) {
            throw new StorageException(StorageException.ErrorCode.MULTIPART_UPLOAD_ERROR,
                "MD5 algorithm not available", e);
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.MULTIPART_UPLOAD_ERROR,
                "Failed to complete multipart upload", e);
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
                throw new StorageException(StorageException.ErrorCode.MULTIPART_UPLOAD_ERROR,
                    "Failed to abort multipart upload", e);
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
                signingKey = "default-signing-key-change-this";
            }
            
            long expiryTime = Instant.now().plusSeconds(expiryInSeconds).toEpochMilli();
            
            // Create a signature
            String dataToSign = bucketName + ":" + objectKey + ":" + expiryTime + ":" + method;
            String signature = hmacSha256(signingKey, dataToSign);
            
            String urlString = baseUrl + bucketName + "/" + objectKey + 
                        "?expiry=" + expiryTime + "&signature=" + signature;
            
            return new StorageUrl(
                new URL(urlString),
                Instant.now().plusSeconds(expiryInSeconds),
                Collections.emptyMap(),
                method == HttpMethod.PUT ? StorageUrl.UrlType.UPLOAD : StorageUrl.UrlType.SIGNED
            );
        } catch (Exception e) {
            throw new StorageException(StorageException.ErrorCode.URL_GENERATION_ERROR,
                "Failed to generate signed URL", e);
        }
    }
    
    @Override
    public StorageUrl generatePublicUrl(String bucketName, String objectKey) throws StorageException {
        try {
            String baseUrl = config.getBaseUrl();
            if (baseUrl == null) {
                baseUrl = "http://localhost:8080/files/";
            }
            
            String urlString = baseUrl + bucketName + "/" + objectKey;
            return new StorageUrl(new URL(urlString), null, Collections.emptyMap(), StorageUrl.UrlType.PUBLIC);
        } catch (Exception e) {
            throw new StorageException(StorageException.ErrorCode.URL_GENERATION_ERROR,
                "Failed to generate public URL", e);
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
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to get object info", e);
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
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to update metadata", e);
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
            Path searchPath = bucketPath.resolve(prefix).normalize();
            
            // Security check
            if (!searchPath.startsWith(bucketPath)) {
                throw new StorageException(StorageException.ErrorCode.INVALID_REQUEST,
                    "Invalid prefix", bucketName, null);
            }
            
            List<FileInfo> objects = new ArrayList<>();
            
            if (Files.exists(searchPath)) {
                Files.walkFileTree(searchPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        // Skip metadata files
                        if (file.toString().endsWith(".meta")) {
                            return FileVisitResult.CONTINUE;
                        }
                        
                        String relativePath = bucketPath.relativize(file).toString().replace('\\', '/');
                        FileInfo fileInfo = new FileInfo(
                            bucketName,
                            relativePath,
                            attrs.size(),
                            attrs.lastModifiedTime().toInstant()
                        );
                        objects.add(fileInfo);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            
            return objects;
        } catch (IOException e) {
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to list objects", e);
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
                FileInfo result = putObject(bucketName, objectKey, data, contentLength, contentType, metadata);
                if (progressCallback != null) {
                    progressCallback.onProgress(contentLength, contentLength);
                }
                return result;
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
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to copy object", e);
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
            throw new StorageException(StorageException.ErrorCode.PROVIDER_ERROR,
                "Failed to move object", e);
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
        fields.put("expires", String.valueOf(Instant.now().plusSeconds(expiryInSeconds).toEpochMilli()));
        
        if (conditions != null) {
            fields.putAll(conditions);
        }
        
        return new PresignedPost(baseUrl, fields);
    }
    
    @Override
    public void close() throws Exception {
        running = false;
        if (cleanupThread != null) {
            cleanupThread.interrupt();
        }
        
        // Clean up all active multipart uploads
        for (String uploadId : new ArrayList<>(activeMultipartUploads.keySet())) {
            try {
                abortMultipartUpload(null, null, uploadId);
            } catch (Exception e) {
                // Log and continue
                System.err.println("Failed to abort multipart upload during close: " + e.getMessage());
            }
        }
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
import com.jjenus.qipi.StorageFactory;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.core.StorageException;
import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.StorageUrl;
import com.jjenus.qipi.model.UploadPartResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Complete demo showing how to use the storage system for user uploads
 */
public class FileUploadDemo {
    
    private final Storage storage;
    
    public FileUploadDemo(Storage storage) {
        this.storage = storage;
    }
    
    /**
     * Upload a file and return URL for database storage
     */
    public UploadResult uploadUserFile(String userId, String fileName, 
                                       InputStream fileData, long fileSize,
                                       String contentType) throws Exception {
        
        // Create user-specific bucket/folder
        String bucketName = "user-" + userId;
        if (!storage.bucketExists(bucketName)) {
            storage.createBucket(bucketName);
        }
        
        // Generate unique file key to avoid collisions
        String fileKey = generateFileKey(userId, fileName);
        
        // Add user metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("userId", userId);
        metadata.put("originalFileName", fileName);
        metadata.put("uploadTime", String.valueOf(System.currentTimeMillis()));
        
        // Upload file
        FileInfo fileInfo = storage.putObject(
            bucketName, fileKey, fileData, fileSize, contentType, metadata
        );
        
        // Generate URLs for different purposes
        StorageUrl publicUrl = storage.generatePublicUrl(bucketName, fileKey);
        StorageUrl signedUrl = storage.generateSignedUrl(
            bucketName, fileKey, Storage.HttpMethod.GET, 3600 // 1 hour expiry
        );
        
        return new UploadResult(
            fileInfo,
            publicUrl,
            signedUrl,
            bucketName + "/" + fileKey  // Path to store in DB
        );
    }
    
    /**
     * Handle large file upload with multipart
     */
    public UploadResult uploadLargeFile(String userId, String fileName,
                                        Path filePath, String contentType) throws Exception {
        
        String bucketName = "user-" + userId;
        if (!storage.bucketExists(bucketName)) {
            storage.createBucket(bucketName);
        }
        
        String fileKey = generateFileKey(userId, fileName);
        long fileSize = Files.size(filePath);
        
        // For files > 100MB, use multipart upload
        if (fileSize > 100 * 1024 * 1024) {
            return multipartUpload(bucketName, fileKey, filePath, contentType, userId);
        } else {
            // Regular upload for smaller files
            try (InputStream is = Files.newInputStream(filePath)) {
                Map<String, String> metadata = new HashMap<>();
                metadata.put("userId", userId);
                metadata.put("originalFileName", fileName);
                
                FileInfo fileInfo = storage.putObject(
                    bucketName, fileKey, is, fileSize, contentType, metadata
                );
                
                StorageUrl signedUrl = storage.generateSignedUrl(
                    bucketName, fileKey, Storage.HttpMethod.GET, 3600
                );
                
                return new UploadResult(
                    fileInfo,
                    null,
                    signedUrl,
                    bucketName + "/" + fileKey
                );
            }
        }
    }
    
    private UploadResult multipartUpload(String bucketName, String fileKey,
                                         Path filePath, String contentType,
                                         String userId) throws Exception {
        
        // Initialize multipart upload
        Map<String, String> metadata = new HashMap<>();
        metadata.put("userId", userId);
        
        String uploadId = storage.initiateMultipartUpload(
            bucketName, fileKey, contentType, metadata
        );
        
        // Upload parts (5MB each)
        long partSize = 5 * 1024 * 1024; // 5MB
        long fileSize = Files.size(filePath);
        int partCount = (int) Math.ceil((double) fileSize / partSize);
        
        List<UploadPartResult> parts = new ArrayList<>();
        
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
                long startPos = (partNumber - 1) * partSize;
                long size = Math.min(partSize, fileSize - startPos);
                
                // Read part data
                byte[] partData = new byte[(int)size];
                raf.seek(startPos);
                raf.readFully(partData);
                
                // Upload part
                try (ByteArrayInputStream bais = new ByteArrayInputStream(partData)) {
                    UploadPartResult partResult = storage.uploadPart(
                        bucketName, fileKey, uploadId, partNumber, bais, size
                    );
                    parts.add(partResult);
                    
                    System.out.println("Uploaded part " + partNumber + " of " + partCount);
                }
            }
        }
        
        // Complete multipart upload
        FileInfo fileInfo = storage.completeMultipartUpload(
            bucketName, fileKey, uploadId, parts
        );
        
        StorageUrl signedUrl = storage.generateSignedUrl(
            bucketName, fileKey, Storage.HttpMethod.GET, 3600
        );
        
        return new UploadResult(
            fileInfo,
            null,
            signedUrl,
            bucketName + "/" + fileKey
        );
    }
    
    /**
     * Download file for user
     */
    public void downloadFile(String storagePath, OutputStream outputStream) throws Exception {
        String[] parts = storagePath.split("/", 2);
        String bucketName = parts[0];
        String fileKey = parts[1];
        
        try (InputStream is = storage.getObject(bucketName, fileKey)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
    }
    
    /**
     * Stream video file (supports range requests)
     */
    public void streamVideo(String storagePath, long start, long end, 
                            OutputStream outputStream) throws Exception {
        String[] parts = storagePath.split("/", 2);
        String bucketName = parts[0];
        String fileKey = parts[1];
        
        try (InputStream is = storage.getObjectRange(bucketName, fileKey, start, end)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
    }
    
    /**
     * Generate temporary access URL for sharing
     */
    public String generateShareLink(String storagePath, long expirySeconds) throws Exception {
        String[] parts = storagePath.split("/", 2);
        String bucketName = parts[0];
        String fileKey = parts[1];
        
        StorageUrl signedUrl = storage.generateSignedUrl(
            bucketName, fileKey, Storage.HttpMethod.GET, expirySeconds
        );
        
        return signedUrl.getUrl().toString();
    }
    
    /**
     * Generate direct upload URL (for browser uploads)
     */
    public StorageUrl generateDirectUploadUrl(String userId, String fileName,
                                              String contentType) throws Exception {
        String bucketName = "user-" + userId;
        String fileKey = generateFileKey(userId, fileName);
        
        return storage.generateUploadUrl(
            bucketName, fileKey, contentType, 3600 // 1 hour to upload
        );
    }
    
    /**
     * List user's files
     */
    public List<FileInfo> listUserFiles(String userId) throws Exception {
        String bucketName = "user-" + userId;
        if (storage.bucketExists(bucketName)) {
            return storage.listObjects(bucketName);
        }
        return new ArrayList<>();
    }
    
    /**
     * Delete user file
     */
    public void deleteFile(String storagePath) throws Exception {
        String[] parts = storagePath.split("/", 2);
        String bucketName = parts[0];
        String fileKey = parts[1];
        
        storage.deleteObject(bucketName, fileKey);
    }
    
    private String generateFileKey(String userId, String fileName) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        return userId + "/" + timestamp + "-" + uniqueId + "-" + fileName;
    }
    
    /**
     * Result of upload operation
     */
    public static class UploadResult {
        private final FileInfo fileInfo;
        private final StorageUrl publicUrl;
        private final StorageUrl signedUrl;
        private final String storagePath;
        
        public UploadResult(FileInfo fileInfo, StorageUrl publicUrl, 
                           StorageUrl signedUrl, String storagePath) {
            this.fileInfo = fileInfo;
            this.publicUrl = publicUrl;
            this.signedUrl = signedUrl;
            this.storagePath = storagePath;
        }
        
        public FileInfo getFileInfo() { return fileInfo; }
        public StorageUrl getPublicUrl() { return publicUrl; }
        public StorageUrl getSignedUrl() { return signedUrl; }
        public String getStoragePath() { return storagePath; }
        
        // This is what you'd store in your database
        public String getDbValue() {
            return storagePath;
        }
    }
    
    public static void main(String[] args) {
        try {
            // Choose your storage provider by changing this properties file
            Storage storage = StorageFactory.createStorageFromProperties("storage-local.properties");
            
            // Or use MinIO for local S3-compatible storage
            // Storage storage = StorageFactory.createStorageFromProperties("storage-minio.properties");
            
            // Or use AWS S3
            // Storage storage = StorageFactory.createStorageFromProperties("storage-s3.properties");
            
            FileUploadDemo demo = new FileUploadDemo(storage);
            
            // Simulate user upload
            String userId = "user123";
            String fileName = "profile-photo.jpg";
            String contentType = "image/jpeg";
            
            // Create a dummy file for demo
            byte[] dummyContent = "This is a test file content".getBytes();
            
            try (ByteArrayInputStream bais = new ByteArrayInputStream(dummyContent)) {
                UploadResult result = demo.uploadUserFile(
                    userId, fileName, bais, dummyContent.length, contentType
                );
                
                System.out.println("File uploaded successfully!");
                System.out.println("Store this in DB: " + result.getDbValue());
                System.out.println("Access URL (valid for 1 hour): " + result.getSignedUrl());
                
                // Later, when user wants to download:
                String storedPath = result.getDbValue();
                
                // Generate share link
                String shareLink = demo.generateShareLink(storedPath, 3600);
                System.out.println("Share link: " + shareLink);
                
                // List user's files
                List<FileInfo> files = demo.listUserFiles(userId);
                System.out.println("User has " + files.size() + " files");
                
                // Clean up
                demo.deleteFile(storedPath);
                System.out.println("File deleted");
            }
            
            storage.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

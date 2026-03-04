package com.jjenus.qipi.demo;

import com.jjenus.qipi.StorageFactory;
import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.StorageUrl;
import com.jjenus.qipi.model.UploadPartResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Complete demo showing how to use the storage system for user uploads
 * Fully compatible with StorageFactory.create(StorageConfig)
 */
public class FileUploadDemo {

    private final Storage storage;

    public FileUploadDemo(Storage storage) {
        this.storage = storage;
    }

    public UploadResult uploadUserFile(String userId, String fileName,
                                       InputStream fileData, long fileSize,
                                       String contentType) throws Exception {

        String bucketName = "user-" + userId;
        if (!storage.bucketExists(bucketName)) {
            storage.createBucket(bucketName);
            System.out.println("Created bucket: " + bucketName);
        }

        String fileKey = generateFileKey(userId, fileName);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("userId", userId);
        metadata.put("originalFileName", fileName);
        metadata.put("uploadTime", String.valueOf(System.currentTimeMillis()));
        metadata.put("contentType", contentType);

        System.out.println("Uploading: " + fileKey + " (" + fileSize + " bytes)");

        FileInfo fileInfo = storage.putObject(
                bucketName, fileKey, fileData, fileSize, contentType, metadata
        );

        System.out.println("Upload complete. ETag: " + fileInfo.getEtag());

        StorageUrl publicUrl = null;
        StorageUrl signedUrl = null;

        try {
            publicUrl = storage.generatePublicUrl(bucketName, fileKey);
            signedUrl = storage.generateSignedUrl(bucketName, fileKey, Storage.HttpMethod.GET, 3600);
        } catch (Exception e) {
            System.out.println("Note: URL generation not configured: " + e.getMessage());
        }

        return new UploadResult(fileInfo, publicUrl, signedUrl, bucketName + "/" + fileKey);
    }

    public UploadResult uploadLargeFile(String userId, String fileName,
                                        Path filePath, String contentType) throws Exception {

        System.out.println("\nHandling large file upload...");

        String bucketName = "user-" + userId;
        if (!storage.bucketExists(bucketName)) {
            storage.createBucket(bucketName);
        }

        String fileKey = generateFileKey(userId, fileName);
        long fileSize = Files.size(filePath);

        System.out.println("File size: " + fileSize + " bytes");

        if (fileSize > 5 * 1024 * 1024) {
            System.out.println("Using multipart upload (file > 5MB)");
            return multipartUpload(bucketName, fileKey, filePath, contentType, userId);
        } else {
            System.out.println("Using regular upload");
            try (InputStream is = Files.newInputStream(filePath)) {
                Map<String, String> metadata = new HashMap<>();
                metadata.put("userId", userId);
                metadata.put("originalFileName", fileName);

                FileInfo fileInfo = storage.putObject(bucketName, fileKey, is, fileSize, contentType, metadata);

                StorageUrl signedUrl = null;
                try {
                    signedUrl = storage.generateSignedUrl(bucketName, fileKey, Storage.HttpMethod.GET, 3600);
                } catch (Exception ignored) {
                }

                return new UploadResult(fileInfo, null, signedUrl, bucketName + "/" + fileKey);
            }
        }
    }

    private UploadResult multipartUpload(String bucketName, String fileKey,
                                         Path filePath, String contentType,
                                         String userId) throws Exception {

        Map<String, String> metadata = new HashMap<>();
        metadata.put("userId", userId);
        metadata.put("uploadMethod", "multipart");

        System.out.println("Initiating multipart upload...");
        String uploadId = storage.initiateMultipartUpload(bucketName, fileKey, contentType, metadata);
        System.out.println("Upload ID: " + uploadId);

        long partSize = 5 * 1024 * 1024;
        long fileSize = Files.size(filePath);
        int partCount = (int) Math.ceil((double) fileSize / partSize);

        List<UploadPartResult> parts = new ArrayList<>();

        System.out.println("Uploading " + partCount + " parts...");

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
                long startPos = (partNumber - 1) * partSize;
                long size = Math.min(partSize, fileSize - startPos);

                byte[] partData = new byte[(int) size];
                raf.seek(startPos);
                raf.readFully(partData);

                try (ByteArrayInputStream bais = new ByteArrayInputStream(partData)) {
                    UploadPartResult partResult = storage.uploadPart(bucketName, fileKey, uploadId, partNumber, bais, size);
                    parts.add(partResult);

                    System.out.print("Part " + partNumber + "/" + partCount + " uploaded\r");
                }
            }
        }

        System.out.println("\nAll parts uploaded. Completing multipart upload...");

        FileInfo fileInfo = storage.completeMultipartUpload(bucketName, fileKey, uploadId, parts);

        System.out.println("Multipart upload complete. Final ETag: " + fileInfo.getEtag());

        StorageUrl signedUrl = null;
        try {
            signedUrl = storage.generateSignedUrl(bucketName, fileKey, Storage.HttpMethod.GET, 3600);
        } catch (Exception ignored) {
        }

        return new UploadResult(fileInfo, null, signedUrl, bucketName + "/" + fileKey);
    }

    public void downloadFile(String storagePath, OutputStream outputStream) throws Exception {
        String[] parts = storagePath.split("/", 2);
        String bucketName = parts[0];
        String fileKey = parts[1];

        System.out.println("Downloading: " + fileKey);

        try (InputStream is = storage.getObject(bucketName, fileKey)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            long totalBytes = 0;

            while ((bytesRead = is.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                totalBytes += bytesRead;
            }

            System.out.println("Downloaded " + totalBytes + " bytes");
        }
    }

    private String generateFileKey(String userId, String fileName) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        return userId + "/" + timestamp + "-" + uniqueId + "-" + fileName;
    }

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
        public String getDbValue() { return storagePath; }
    }

    public static void main(String[] args) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("QIPI STORAGE DEMO");
        System.out.println("=".repeat(60));

        Storage storage = null;

        try {
            // Manual storage config, fully self-contained
            storage = StorageFactory.create(
                    new StorageConfig.Builder()
                            .provider(StorageConfig.ProviderType.LOCAL)
                            .basePath("./qipi-demo-data")
                            .baseUrl("file://" + Paths.get("./qipi-demo-data").toAbsolutePath() + "/")
                            .signingKey("demo-signing-key-2026")
                            .build()
            );

            System.out.println("Storage initialized");

            FileUploadDemo demo = new FileUploadDemo(storage);
            String userId = "demo-user-" + System.currentTimeMillis() % 10000;
            System.out.println("\nDemo user: " + userId);

        } catch (Exception e) {
            System.err.println("\nDEMO FAILED: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (storage != null) {
                try {
                    storage.close();
                    System.out.println("\nStorage connection closed");
                } catch (Exception e) {
                    System.err.println("Error closing storage: " + e.getMessage());
                }
            }
        }
    }
}
package com.jjenus.qipi.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Comprehensive file information
 */
public class FileInfo {
    private String bucketName;
    private String key;
    private String versionId;
    private long size;
    private String etag;
    private Instant lastModified;
    private String contentType;
    private Map<String, String> userMetadata;
    private StorageClass storageClass;
    private EncryptionStatus encryption;
    private boolean isMultipart;
    
    public enum StorageClass {
        STANDARD, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE
    }
    
    public enum EncryptionStatus {
        NONE, AES256, AWS_KMS
    }
    
    public FileInfo() {
        this.userMetadata = new HashMap<>();
    }
    
    public FileInfo(String bucketName, String key, long size, Instant lastModified) {
        this();
        this.bucketName = bucketName;
        this.key = key;
        this.size = size;
        this.lastModified = lastModified;
    }
    
    // Getters and Setters
    public String getBucketName() { return bucketName; }
    public void setBucketName(String bucketName) { this.bucketName = bucketName; }
    
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    
    public String getVersionId() { return versionId; }
    public void setVersionId(String versionId) { this.versionId = versionId; }
    
    public long getSize() { return size; }
    public void setSize(long size) { this.size = size; }
    
    public String getEtag() { return etag; }
    public void setEtag(String etag) { this.etag = etag; }
    
    public Instant getLastModified() { return lastModified; }
    public void setLastModified(Instant lastModified) { this.lastModified = lastModified; }
    
    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }
    
    public Map<String, String> getUserMetadata() { return userMetadata; }
    public void setUserMetadata(Map<String, String> userMetadata) { 
        this.userMetadata = userMetadata; 
    }
    
    public StorageClass getStorageClass() { return storageClass; }
    public void setStorageClass(StorageClass storageClass) { this.storageClass = storageClass; }
    
    public EncryptionStatus getEncryption() { return encryption; }
    public void setEncryption(EncryptionStatus encryption) { this.encryption = encryption; }
    
    public boolean isMultipart() { return isMultipart; }
    public void setMultipart(boolean multipart) { isMultipart = multipart; }
}

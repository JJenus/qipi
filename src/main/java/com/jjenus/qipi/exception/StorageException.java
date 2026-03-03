package com.jjenus.qipi.exception;

public class StorageException extends Exception {
    private final ErrorCode errorCode;
    private final String bucketName;
    private final String objectKey;
    
    public enum ErrorCode {
        BUCKET_ALREADY_EXISTS,
        BUCKET_NOT_FOUND,
        BUCKET_NOT_EMPTY,
        OBJECT_NOT_FOUND,
        OBJECT_ALREADY_EXISTS,
        ACCESS_DENIED,
        INVALID_REQUEST,
        MULTIPART_UPLOAD_ERROR,
        URL_GENERATION_ERROR,
        NETWORK_ERROR,
        SERVER_ERROR,
        VALIDATION_ERROR,
        PROVIDER_ERROR
    }
    
    public StorageException(String message) {
        super(message);
        this.errorCode = ErrorCode.SERVER_ERROR;
        this.bucketName = null;
        this.objectKey = null;
    }
    
    public StorageException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCode.SERVER_ERROR;
        this.bucketName = null;
        this.objectKey = null;
    }
    
    public StorageException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.bucketName = null;
        this.objectKey = null;
    }
    
    public StorageException(ErrorCode errorCode, String message, String bucketName, String objectKey) {
        super(message);
        this.errorCode = errorCode;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
    }
    
    public StorageException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.bucketName = null;
        this.objectKey = null;
    }
    
    public ErrorCode getErrorCode() { return errorCode; }
    public String getBucketName() { return bucketName; }
    public String getObjectKey() { return objectKey; }
}
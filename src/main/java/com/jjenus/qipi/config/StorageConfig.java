package com.jjenus.qipi.config;

import java.io.InputStream;
import java.util.Properties;

public class StorageConfig {
    
    public enum ProviderType {
        LOCAL,          // Local filesystem
        AWS_S3,         // Amazon S3
        MINIO,          // MinIO (S3-compatible)
        GCS,            // Google Cloud Storage (future)
        AZURE_BLOB      // Azure Blob Storage (future)
    }
    
    private final ProviderType providerType;
    private final String basePath;
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final boolean pathStyleAccess;
    private final boolean useHttps;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final int maxConnections;
    private final String bucketPrefix;
    private final String baseUrl;
    private final String signingKey;
    private final long multipartMinPartSize;
    private final long multipartMaxPartSize;
    private final int multipartMaxParts;
    private final long defaultUrlExpirySeconds;
    
    private StorageConfig(Builder builder) {
        // Validate based on provider type
        validateConfig(builder);
        
        this.providerType = builder.providerType;
        this.basePath = builder.basePath;
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.sessionToken = builder.sessionToken;
        this.pathStyleAccess = builder.pathStyleAccess;
        this.useHttps = builder.useHttps;
        this.connectionTimeout = builder.connectionTimeout;
        this.socketTimeout = builder.socketTimeout;
        this.maxConnections = builder.maxConnections;
        this.bucketPrefix = builder.bucketPrefix;
        this.baseUrl = builder.baseUrl;
        this.signingKey = builder.signingKey;
        this.multipartMinPartSize = builder.multipartMinPartSize;
        this.multipartMaxPartSize = builder.multipartMaxPartSize;
        this.multipartMaxParts = builder.multipartMaxParts;
        this.defaultUrlExpirySeconds = builder.defaultUrlExpirySeconds;
    }
    
    private void validateConfig(Builder builder) {
        switch (builder.providerType) {
            case LOCAL:
                if (builder.basePath == null || builder.basePath.trim().isEmpty()) {
                    throw new IllegalArgumentException("basePath is required for LOCAL storage");
                }
                break;
            case AWS_S3:
                if (builder.region == null || builder.region.trim().isEmpty()) {
                    throw new IllegalArgumentException("region is required for AWS_S3 storage");
                }
                if (builder.accessKey == null || builder.secretKey == null) {
                    throw new IllegalArgumentException("accessKey and secretKey are required for AWS_S3 storage");
                }
                break;
            case MINIO:
                if (builder.endpoint == null || builder.endpoint.trim().isEmpty()) {
                    throw new IllegalArgumentException("endpoint is required for MINIO storage");
                }
                if (builder.accessKey == null || builder.secretKey == null) {
                    throw new IllegalArgumentException("accessKey and secretKey are required for MINIO storage");
                }
                break;
        }
    }
    
    public static class Builder {
        private ProviderType providerType = ProviderType.LOCAL;
        private String basePath = "./storage";
        private String endpoint;
        private String region = "us-east-1";
        private String accessKey;
        private String secretKey;
        private String sessionToken;
        private boolean pathStyleAccess = false;
        private boolean useHttps = true;
        private int connectionTimeout = 30000;
        private int socketTimeout = 60000;
        private int maxConnections = 50;
        private String bucketPrefix;
        private String baseUrl;
        private String signingKey;
        private long multipartMinPartSize = 5 * 1024 * 1024;      // 5MB
        private long multipartMaxPartSize = 5L * 1024 * 1024 * 1024; // 5GB
        private int multipartMaxParts = 10000;
        private long defaultUrlExpirySeconds = 3600;
        
        public Builder provider(ProviderType type) {
            this.providerType = type;
            return this;
        }
        
        public Builder basePath(String basePath) {
            this.basePath = basePath;
            return this;
        }
        
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }
        
        public Builder region(String region) {
            this.region = region;
            return this;
        }
        
        public Builder credentials(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            return this;
        }
        
        public Builder credentials(String accessKey, String secretKey, String sessionToken) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.sessionToken = sessionToken;
            return this;
        }
        
        public Builder pathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }
        
        public Builder useHttps(boolean useHttps) {
            this.useHttps = useHttps;
            return this;
        }
        
        public Builder timeouts(int connectionTimeout, int socketTimeout) {
            this.connectionTimeout = connectionTimeout;
            this.socketTimeout = socketTimeout;
            return this;
        }
        
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }
        
        public Builder bucketPrefix(String bucketPrefix) {
            this.bucketPrefix = bucketPrefix;
            return this;
        }
        
        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }
        
        public Builder signingKey(String signingKey) {
            this.signingKey = signingKey;
            return this;
        }
        
        public Builder multipartSettings(long minPartSize, long maxPartSize, int maxParts) {
            this.multipartMinPartSize = minPartSize;
            this.multipartMaxPartSize = maxPartSize;
            this.multipartMaxParts = maxParts;
            return this;
        }
        
        public Builder defaultUrlExpirySeconds(long seconds) {
            this.defaultUrlExpirySeconds = seconds;
            return this;
        }
        
        public StorageConfig build() {
            return new StorageConfig(this);
        }
    }
    
    public static StorageConfig fromProperties(InputStream propsStream) throws Exception {
        Properties props = new Properties();
        props.load(propsStream);
        
        Builder builder = new Builder();
        
        String provider = props.getProperty("storage.provider", "LOCAL");
        try {
            builder.provider(ProviderType.valueOf(provider.toUpperCase()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid provider type: " + provider);
        }
        
        if (props.containsKey("storage.basePath")) {
            builder.basePath(props.getProperty("storage.basePath"));
        }
        
        if (props.containsKey("storage.endpoint")) {
            builder.endpoint(props.getProperty("storage.endpoint"));
        }
        
        if (props.containsKey("storage.region")) {
            builder.region(props.getProperty("storage.region"));
        }
        
        if (props.containsKey("storage.accessKey") && props.containsKey("storage.secretKey")) {
            builder.credentials(
                props.getProperty("storage.accessKey"),
                props.getProperty("storage.secretKey"),
                props.getProperty("storage.sessionToken", null)
            );
        }
        
        if (props.containsKey("storage.pathStyleAccess")) {
            builder.pathStyleAccess(Boolean.parseBoolean(props.getProperty("storage.pathStyleAccess")));
        }
        
        if (props.containsKey("storage.useHttps")) {
            builder.useHttps(Boolean.parseBoolean(props.getProperty("storage.useHttps")));
        }
        
        if (props.containsKey("storage.baseUrl")) {
            builder.baseUrl(props.getProperty("storage.baseUrl"));
        }
        
        if (props.containsKey("storage.signingKey")) {
            builder.signingKey(props.getProperty("storage.signingKey"));
        }
        
        if (props.containsKey("storage.bucketPrefix")) {
            builder.bucketPrefix(props.getProperty("storage.bucketPrefix"));
        }
        
        return builder.build();
    }
    
    // Getters
    public ProviderType getProviderType() { return providerType; }
    public String getBasePath() { return basePath; }
    public String getEndpoint() { return endpoint; }
    public String getRegion() { return region; }
    public String getAccessKey() { return accessKey; }
    public String getSecretKey() { return secretKey; }
    public String getSessionToken() { return sessionToken; }
    public boolean isPathStyleAccess() { return pathStyleAccess; }
    public boolean isUseHttps() { return useHttps; }
    public int getConnectionTimeout() { return connectionTimeout; }
    public int getSocketTimeout() { return socketTimeout; }
    public int getMaxConnections() { return maxConnections; }
    public String getBucketPrefix() { return bucketPrefix; }
    public String getBaseUrl() { return baseUrl; }
    public String getSigningKey() { return signingKey; }
    public long getMultipartMinPartSize() { return multipartMinPartSize; }
    public long getMultipartMaxPartSize() { return multipartMaxPartSize; }
    public int getMultipartMaxParts() { return multipartMaxParts; }
    public long getDefaultUrlExpirySeconds() { return defaultUrlExpirySeconds; }
}
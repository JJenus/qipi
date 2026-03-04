package com.jjenus.qipi.config;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * Storage configuration.
 *
 * Note: Environment variable resolution (${ENV:default}) must be handled
 * by the host application's configuration system before passing values here.
 * This class does not perform environment discovery or file loading.
 */
public final class StorageConfig {

    public enum ProviderType {
        LOCAL, AWS_S3, MINIO, GCS, AZURE_BLOB
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
        validate(builder);

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

    private static void validate(Builder b) {
        if (b.providerType == null) {
            throw new IllegalArgumentException("Provider type must be specified");
        }

        switch (b.providerType) {
            case LOCAL:
                if (b.basePath == null || b.basePath.trim().isEmpty()) {
                    throw new IllegalArgumentException("basePath is required for LOCAL storage");
                }
                break;

            case AWS_S3:
                if (b.region == null || b.region.trim().isEmpty()) {
                    throw new IllegalArgumentException("region is required for AWS_S3 storage");
                }
                if (b.accessKey == null || b.secretKey == null) {
                    throw new IllegalArgumentException("accessKey and secretKey are required for AWS_S3 storage");
                }
                break;

            case MINIO:
                if (b.endpoint == null || b.endpoint.trim().isEmpty()) {
                    throw new IllegalArgumentException("endpoint is required for MINIO storage");
                }
                if (b.accessKey == null || b.secretKey == null) {
                    throw new IllegalArgumentException("accessKey and secretKey are required for MINIO storage");
                }
                break;

            default:
                break;
        }
    }

    public static final class Builder {

        private ProviderType providerType;
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
        private long multipartMinPartSize = 5 * 1024 * 1024;
        private long multipartMaxPartSize = 5L * 1024 * 1024 * 1024;
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

        public Builder sessionToken(String sessionToken) {
            this.sessionToken = sessionToken;
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

        public Builder fromProperties(Properties props) {
            return fromResolver(props::getProperty);
        }

        public Builder fromMap(Map<String, String> map) {
            return fromResolver(map::get);
        }

        public Builder fromResolver(Function<String, String> resolver) {

            String provider = resolver.apply("storage.provider");
            if (provider != null) {
                this.providerType = ProviderType.valueOf(provider.toUpperCase());
            }

            this.basePath = value(resolver, "storage.basePath", this.basePath);
            this.endpoint = value(resolver, "storage.endpoint", this.endpoint);
            this.region = value(resolver, "storage.region", this.region);
            this.accessKey = value(resolver, "storage.accessKey", this.accessKey);
            this.secretKey = value(resolver, "storage.secretKey", this.secretKey);
            this.sessionToken = value(resolver, "storage.sessionToken", this.sessionToken);
            this.baseUrl = value(resolver, "storage.baseUrl", this.baseUrl);
            this.signingKey = value(resolver, "storage.signingKey", this.signingKey);
            this.bucketPrefix = value(resolver, "storage.bucketPrefix", this.bucketPrefix);

            String pathStyle = resolver.apply("storage.pathStyleAccess");
            if (pathStyle != null) {
                this.pathStyleAccess = Boolean.parseBoolean(pathStyle);
            }

            String https = resolver.apply("storage.useHttps");
            if (https != null) {
                this.useHttps = Boolean.parseBoolean(https);
            }

            String connTimeout = resolver.apply("storage.connectionTimeout");
            if (connTimeout != null) {
                this.connectionTimeout = Integer.parseInt(connTimeout);
            }

            String sockTimeout = resolver.apply("storage.socketTimeout");
            if (sockTimeout != null) {
                this.socketTimeout = Integer.parseInt(sockTimeout);
            }

            String maxConn = resolver.apply("storage.maxConnections");
            if (maxConn != null) {
                this.maxConnections = Integer.parseInt(maxConn);
            }

            String minPart = resolver.apply("storage.multipart.minPartSize");
            if (minPart != null) {
                this.multipartMinPartSize = Long.parseLong(minPart);
            }

            String maxPart = resolver.apply("storage.multipart.maxPartSize");
            if (maxPart != null) {
                this.multipartMaxPartSize = Long.parseLong(maxPart);
            }

            String maxParts = resolver.apply("storage.multipart.maxParts");
            if (maxParts != null) {
                this.multipartMaxParts = Integer.parseInt(maxParts);
            }

            String expiry = resolver.apply("storage.defaultUrlExpirySeconds");
            if (expiry != null) {
                this.defaultUrlExpirySeconds = Long.parseLong(expiry);
            }

            return this;
        }

        private String value(Function<String, String> resolver, String key, String current) {
            String v = resolver.apply(key);
            return v != null ? v : current;
        }

        public StorageConfig build() {
            return new StorageConfig(this);
        }
    }

    /**
     * Convenience plug-and-play entry point.
     * Reads from JVM system properties first, then OS environment variables.
     */
    public static StorageConfig fromSystemEnvironment() {
        return new Builder()
                .fromResolver(key -> {
                    String v = System.getProperty(key);
                    if (v != null) {
                        return v;
                    }
                    return System.getenv(key.replace('.', '_').toUpperCase());
                })
                .build();
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
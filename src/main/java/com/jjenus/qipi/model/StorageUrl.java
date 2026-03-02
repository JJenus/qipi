package com.jjenus.qipi.model;

import java.net.URL;
import java.time.Instant;
import java.util.Map;

/**
 * Represents a storage URL with metadata
 */
public class StorageUrl {
    private final URL url;
    private final Instant expirationTime;
    private final Map<String, String> signedHeaders;
    private final UrlType type;
    
    public enum UrlType {
        PUBLIC,
        SIGNED,
        UPLOAD
    }
    
    public StorageUrl(URL url, Instant expirationTime, Map<String, String> signedHeaders, UrlType type) {
        this.url = url;
        this.expirationTime = expirationTime;
        this.signedHeaders = signedHeaders;
        this.type = type;
    }
    
    public URL getUrl() { return url; }
    public Instant getExpirationTime() { return expirationTime; }
    public Map<String, String> getSignedHeaders() { return signedHeaders; }
    public UrlType getType() { return type; }
    public boolean isExpired() {
        return expirationTime != null && Instant.now().isAfter(expirationTime);
    }
    
    @Override
    public String toString() {
        return url.toString();
    }
}

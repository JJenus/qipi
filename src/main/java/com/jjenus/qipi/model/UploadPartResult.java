package com.jjenus.qipi.model;

/**
 * Result of uploading a part in multipart upload
 */
public class UploadPartResult {
    private final int partNumber;
    private final String etag;
    private final long partSize;
    
    public UploadPartResult(int partNumber, String etag, long partSize) {
        this.partNumber = partNumber;
        this.etag = etag;
        this.partSize = partSize;
    }
    
    public int getPartNumber() { return partNumber; }
    public String getEtag() { return etag; }
    public long getPartSize() { return partSize; }
    
    @Override
    public String toString() {
        return String.format("Part #%d: %s (%d bytes)", partNumber, etag, partSize);
    }
}

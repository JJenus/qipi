# qipi - Pluggable Object Storage

A flexible, S3-compatible storage system that works with local filesystem, MinIO, and AWS S3 using the same unified API.

## Features

- **Pluggable Architecture** - Switch between local, MinIO, and AWS S3 without code changes
- **Multipart Upload** - Support for large files (videos, music, etc.) with automatic part management
- **URL Generation** - Public URLs, signed URLs (temporary access), and upload URLs
- **Range Requests** - Video/audio streaming support with partial content delivery
- **Metadata Management** - Store and retrieve custom metadata with files
- **Async Operations** - Non-blocking uploads/downloads with progress tracking
- **Batch Operations** - Delete multiple files at once
- **Copy/Move** - Operations within and across buckets
- **Path Traversal Protection** - Security built-in to prevent directory traversal attacks
- **Automatic Cleanup** - Stale multipart uploads are automatically cleaned up

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Your Application                        │
├─────────────────────────────────────────────────────────────┤
│                        Storage API                           │
│                     (com.jjenus.qipi.core)                   │
├─────────────────────────────────────────────────────────────┤
│                        StorageFactory                        │
├─────────────────────────────────────────────────────────────┤
│    Local          MinIO          AWS S3        Azure/GCS    │
│  Provider      Provider       Provider        (Coming Soon) │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
qipi/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── jjenus/
│   │               └── qipi/
│   │                   ├── StorageFactory.java
│   │                   ├── config/
│   │                   │   └── StorageConfig.java
│   │                   ├── core/
│   │                   │   └── Storage.java
│   │                   ├── exception/
│   │                   │   └── StorageException.java
│   │                   ├── model/
│   │                   │   ├── FileInfo.java
│   │                   │   ├── StorageUrl.java
│   │                   │   └── UploadPartResult.java
│   │                   └── providers/
│   │                       └── LocalStorageProvider.java
│   └── test/
│       └── java/
│           └── com/
│               └── jjenus/
│                   └── qipi/
│                       ├── StorageFactoryTest.java
│                       ├── config/
│                       │   └── StorageConfigTest.java
│                       └── providers/
│                           └── LocalStorageProviderTest.java
```

## Quick Start

### 1. Add Dependency

Add this to your `pom.xml`:

```xml
<dependency>
    <groupId>com.jjenus</groupId>
    <artifactId>qipi</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Configuration

#### Option A: Programmatic Configuration

```java
StorageConfig config = new StorageConfig.Builder()
    .provider(StorageConfig.ProviderType.LOCAL)
    .basePath("./data/storage")
    .baseUrl("http://localhost:8080/files/")
    .signingKey("your-signing-key")
    .build();

Storage storage = StorageFactory.create(config);
```

#### Option B: Properties File

Create `src/main/resources/storage-local.properties`:

```properties
storage.provider=LOCAL
storage.basePath=./data/storage
storage.baseUrl=http://localhost:8080/files/
storage.signingKey=your-local-signing-key-change-this
```

Load and use:

```java
Properties props = new Properties();
try (InputStream input = Files.newInputStream(Paths.get("storage-local.properties"))) {
    props.load(input);
}

StorageConfig config = new StorageConfig.Builder()
    .fromProperties(props)
    .build();

Storage storage = StorageFactory.create(config);
```

### 3. Basic Usage

```java
import com.jjenus.qipi.StorageFactory;
import com.jjenus.qipi.config.StorageConfig;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.StorageUrl;

// Initialize storage
Storage storage = StorageFactory.create(config);

// Create bucket
String bucketName = "user-123";
if (!storage.bucketExists(bucketName)) {
    storage.createBucket(bucketName);
}

// Upload file
String fileKey = "photos/profile.jpg";
Map<String, String> metadata = new HashMap<>();
metadata.put("userId", "123");
metadata.put("uploadDate", String.valueOf(System.currentTimeMillis()));

try (InputStream fileData = new FileInputStream("photo.jpg")) {
    FileInfo info = storage.putObject(
        bucketName,
        fileKey,
        fileData,
        fileSize,
        "image/jpeg",
        metadata
    );
    
    System.out.println("Uploaded: " + info.getEtag());
}

// Generate signed URL (temporary access)
StorageUrl url = storage.generateSignedUrl(
    bucketName,
    fileKey,
    Storage.HttpMethod.GET,
    3600 // 1 hour
);

System.out.println("Access URL: " + url.getUrl());

// Download file
try (InputStream downloaded = storage.getObject(bucketName, fileKey)) {
    // Process the stream
    byte[] data = downloaded.readAllBytes();
}

// Delete file
storage.deleteObject(bucketName, fileKey);

// Close storage
storage.close();
```

## Advanced Usage

### Multipart Upload for Large Files

```java
// Initiate multipart upload
String uploadId = storage.initiateMultipartUpload(
    bucketName,
    "videos/large-video.mp4",
    "video/mp4",
    metadata
);

// Upload parts
List<UploadPartResult> parts = new ArrayList<>();
int partNumber = 1;
byte[] buffer = new byte[5 * 1024 * 1024]; // 5MB parts

try (InputStream in = new FileInputStream("large-video.mp4")) {
    int bytesRead;
    while ((bytesRead = in.read(buffer)) > 0) {
        UploadPartResult part = storage.uploadPart(
            bucketName,
            "videos/large-video.mp4",
            uploadId,
            partNumber++,
            new ByteArrayInputStream(buffer, 0, bytesRead),
            bytesRead
        );
        parts.add(part);
    }
}

// Complete multipart upload
FileInfo completed = storage.completeMultipartUpload(
    bucketName,
    "videos/large-video.mp4",
    uploadId,
    parts
);
```

### Async Operations with Progress

```java
CompletableFuture<FileInfo> future = storage.putObjectAsync(
    bucketName,
    "large-file.zip",
    inputStream,
    contentLength,
    "application/zip",
    metadata,
    (bytesTransferred, totalBytes) -> {
        double percent = (bytesTransferred * 100.0) / totalBytes;
        System.out.printf("Progress: %.2f%%\n", percent);
    }
);

future.thenAccept(fileInfo -> {
    System.out.println("Upload complete: " + fileInfo.getEtag());
});
```

### Range Requests (Video Streaming)

```java
// Get only bytes 1000-1999 of a file
try (InputStream rangeStream = storage.getObjectRange(
    bucketName,
    "videos/movie.mp4",
    1000,
    1999
)) {
    // Stream this range to client
    rangeStream.transferTo(responseOutputStream);
}
```

### Copy and Move Operations

```java
// Copy file
FileInfo copied = storage.copyObject(
    "source-bucket",
    "path/to/file.txt",
    "dest-bucket",
    "new/path/file.txt"
);

// Move file
FileInfo moved = storage.moveObject(
    "source-bucket",
    "path/to/file.txt",
    "dest-bucket",
    "new/path/file.txt"
);
```

### Batch Operations

```java
// Delete multiple files at once
List<String> keys = Arrays.asList(
    "file1.txt",
    "file2.txt",
    "file3.txt"
);
storage.deleteObjects("bucket-name", keys);
```

## Configuration Reference

| Property | Description | Default | Required |
|----------|-------------|---------|----------|
| `storage.provider` | Provider type (LOCAL, AWS_S3, MINIO, GCS, AZURE_BLOB) | - | Yes |
| `storage.basePath` | Local storage path | ./storage | For LOCAL |
| `storage.endpoint` | MinIO endpoint | - | For MINIO |
| `storage.region` | AWS region | us-east-1 | For AWS_S3 |
| `storage.accessKey` | Access key | - | For AWS_S3/MINIO |
| `storage.secretKey` | Secret key | - | For AWS_S3/MINIO |
| `storage.sessionToken` | Session token | - | No |
| `storage.pathStyleAccess` | Use path-style URLs | false | No |
| `storage.useHttps` | Use HTTPS | true | No |
| `storage.connectionTimeout` | Connection timeout (ms) | 30000 | No |
| `storage.socketTimeout` | Socket timeout (ms) | 60000 | No |
| `storage.maxConnections` | Max connections | 50 | No |
| `storage.bucketPrefix` | Prefix for all buckets | - | No |
| `storage.baseUrl` | Base URL for public access | - | No |
| `storage.signingKey` | Key for signed URLs | - | No |
| `storage.multipart.minPartSize` | Min part size (bytes) | 5242880 (5MB) | No |
| `storage.multipart.maxPartSize` | Max part size (bytes) | 5368709120 (5GB) | No |
| `storage.multipart.maxParts` | Max parts per upload | 10000 | No |
| `storage.defaultUrlExpirySeconds` | Default URL expiry | 3600 | No |

## Error Handling

```java
try {
    storage.getObject("nonexistent-bucket", "file.txt");
} catch (StorageException e) {
    switch (e.getErrorCode()) {
        case BUCKET_NOT_FOUND:
            System.out.println("Bucket doesn't exist");
            break;
        case OBJECT_NOT_FOUND:
            System.out.println("File doesn't exist");
            break;
        case ACCESS_DENIED:
            System.out.println("Permission denied");
            break;
        default:
            System.out.println("Storage error: " + e.getMessage());
    }
}
```

## Testing

Run the test suite:

```bash
mvn clean test
```

For integration tests with actual storage providers:

```bash
mvn verify -Pintegration-tests
```

## Provider Support

| Provider | Status | Features |
|----------|--------|----------|
| LOCAL | ✅ Complete | All features supported |
| MINIO | 🚧 In Progress | Coming soon |
| AWS S3 | 🚧 In Progress | Coming soon |
| GCS | 📅 Planned | Future release |
| Azure Blob | 📅 Planned | Future release |

## Requirements

- Java 17 or higher
- Maven 3.6+ (for building)

## Building from Source

```bash
git clone https://github.com/jjenus/qipi.git
cd qipi
mvn clean install
```

## License

MIT License

Copyright (c) 2026 qipi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
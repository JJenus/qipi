# qipi - Pluggable Object Storage (update required)

A flexible, S3-compatible storage system that works with local filesystem, MinIO, and AWS S3 using the same code.

## Features

- Pluggable Architecture - Switch between local, MinIO, and AWS S3 without code changes
- Multipart Upload - Support for large files (videos, music, etc.)
- URL Generation - Public URLs, signed URLs, and upload URLs
- Range Requests - Video streaming support
- Metadata Management - Store custom metadata with files
- Async Operations - Non-blocking uploads/downloads with progress tracking
- Batch Operations - Delete multiple files at once
- Copy/Move - Operations within and across buckets

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

### 2. Choose Your Storage

**Option A: Local Storage**

Create `src/main/resources/storage.properties`:

```properties
storage.provider=LOCAL
storage.basePath=./data/storage
storage.baseUrl=http://localhost:8080/files/
storage.signingKey=your-local-signing-key-change-this
```

**Option B: MinIO**

```properties
storage.provider=MINIO
storage.endpoint=localhost:9000
storage.region=us-east-1
storage.accessKey=minioadmin
storage.secretKey=minioadmin
storage.pathStyleAccess=true
storage.useHttps=false
```

**Option C: AWS S3**

```properties
storage.provider=AWS_S3
storage.region=us-east-1
storage.accessKey=YOUR_AWS_ACCESS_KEY
storage.secretKey=YOUR_AWS_SECRET_KEY
```

### 3. Use the Storage

```java
import com.jjenus.qipi.StorageFactory;
import com.jjenus.qipi.core.Storage;
import com.jjenus.qipi.model.FileInfo;
import com.jjenus.qipi.model.StorageUrl;

Storage storage = StorageFactory.createStorageFromProperties("storage.properties");

String userId = "user123";
String bucketName = "user-" + userId;
String fileKey = "photos/profile.jpg";

if (!storage.bucketExists(bucketName)) {
    storage.createBucket(bucketName);
}

try (InputStream fileData = new FileInputStream("photo.jpg")) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("userId", userId);
    metadata.put("originalFileName", "photo.jpg");

    FileInfo info = storage.putObject(
        bucketName,
        fileKey,
        fileData,
        fileSize,
        "image/jpeg",
        metadata
    );

    StorageUrl url = storage.generateSignedUrl(
        bucketName,
        fileKey,
        Storage.HttpMethod.GET,
        3600
    );

    String dbPath = bucketName + "/" + fileKey;
    System.out.println("Store in DB: " + dbPath);
    System.out.println("Access URL: " + url.getUrl());
}

storage.close();
```

## License

MIT License - Copyright (c) 2026 qipi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files...
EOF
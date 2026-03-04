#!/bin/bash

echo "==================================="
echo "  QIPI Storage Demo - Termux/Java 17"
echo "==================================="

# move to project root
cd ..

#copy demo to project app path 
cp -r demo ./src/main/java/com/jjenus/qipi/

# Disable Jansi/JLine to avoid native library issues
export MAVEN_OPTS="-Dorg.jline.terminal.dumb=true -Djansi.passthrough=true"

# Create data directory
mkdir -p ./qipi-demo-data

# Clean and compile
echo -e "\n📦 Compiling..."
mvn clean compile

if [ $? -ne 0 ]; then
    echo "❌ Compilation failed!"
    exit 1
fi

echo "✅ Compilation successful"

# Create a properties file for the demo
cat > target/classes/storage-demo.properties << EOF
# Demo Storage Configuration
storage.provider=LOCAL
storage.basePath=./qipi-demo-data
storage.baseUrl=file://$(pwd)/qipi-demo-data/
storage.signingKey=demo-signing-key-$(date +%s)
EOF

# Run the demo
echo -e "\n🎬 Running FileUploadDemo...\n"
java -cp target/classes \
     -Dorg.jline.terminal.dumb=true \
     -Djansi.passthrough=true \
     -Dfile.encoding=UTF-8 \
     com.jjenus.qipi.demo.FileUploadDemo \
     storage-demo.properties

echo -e "\n==================================="

# clean up demo
rm -r ./src/main/java/com/jjenus/qipi/demo

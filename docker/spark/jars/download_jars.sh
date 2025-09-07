#!/bin/sh
set -e
echo "This script attempts to download Delta + Hadoop AWS jars to ./docker/spark/jars."
echo "You should inspect versions and Maven Central URLs before running in production."
# Example Maven Central coordinates (change versions as needed)
echo "Downloading delta-core_2.12 and hadoop-aws..."
# The environment where this repo is packaged may not have network access.
# Replace the URLs below with current Maven Central links if running locally.
# curl -L -o delta-core_2.12-2.4.0.jar "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar"
# curl -L -o hadoop-aws-3.3.4.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
# curl -L -o aws-java-sdk-bundle-1.12.406.jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.406/aws-java-sdk-bundle-1.12.406.jar"
echo "Download commands commented out by default. Edit this script to enable."

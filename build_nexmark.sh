#!/bin/bash

git pull

./build_kafka.sh

cd sdks/java/nexmark 
rm build/libs/beam-sdks*
../../../gradlew build shadowJar -x test

mvn install:install-file -Dfile=build/libs/beam-sdks-java-nexmark-2.6.0-SNAPSHOT.jar \
-DgroupId=org.apache.beam \
-DartifactId=beam-sdks-java-nexmark \
-Dversion=2.6.0-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true

pushd ../nexmark-source-gen/
mvn clean install
popd
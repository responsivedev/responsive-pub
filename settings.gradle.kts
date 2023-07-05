/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

rootProject.name = "responsive-pub"

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}

include("kafka-client")
include("controller-api")
include("operator")
include("tools")

include("responsive-test-utils")

dependencyResolutionManagement {

    versionCatalogs {
        create("libs") {
            version("jackson", "2.14.2")
            version("kafka", "3.4.0")
            version("scylla", "4.15.0.0")
            version("javaoperatorsdk", "4.3.0")
            version("grpc", "1.52.1")
            version("protobuf-java", "3.22.3")
            version("slf4j", "1.7.5")
            version("log4j2", "2.20.0")

            library("jackson", "com.fasterxml.jackson.datatype", "jackson-datatype-jdk8").versionRef("jackson")

            library("kafka-clients", "org.apache.kafka", "kafka-clients").versionRef("kafka")
            library("kafka-streams", "org.apache.kafka", "kafka-streams").versionRef("kafka")

            library("scylla-driver-core", "com.scylladb", "java-driver-core").versionRef("scylla")
            library("scylla-query-builder", "com.scylladb", "java-driver-query-builder").versionRef("scylla")
            library("scylla-mapper-runtime", "com.scylladb", "java-driver-mapper-runtime").versionRef("scylla")
            bundle("scylla", listOf("scylla-driver-core", "scylla-query-builder", "scylla-mapper-runtime"))

            library("javaoperatorsdk", "io.javaoperatorsdk", "operator-framework").versionRef("javaoperatorsdk")

            library("grpc-netty", "io.grpc", "grpc-netty").versionRef("grpc")
            library("grpc-protobuf", "io.grpc", "grpc-protobuf").versionRef("grpc")
            library("grpc-stub", "io.grpc", "grpc-stub").versionRef("grpc")
            // Workaround for @javax.annotation.Generated
            // see: https://github.com/grpc/grpc-java/issues/3633
            library("javax-annotation-api", "javax.annotation", "javax.annotation-api").version("1.3.1")
            bundle("grpc", listOf("grpc-netty", "grpc-protobuf", "grpc-stub", "javax-annotation-api"))

            library("protobuf-java-util", "com.google.protobuf", "protobuf-java-util").versionRef("protobuf-java")
            library("crd-generator-atp", "io.fabric8", "crd-generator-apt").version("6.5.1")

            library("commons-cli", "commons-cli:commons-cli:1.5.0")
            library("commons-beanutils", "commons-beanutils:commons-beanutils:1.9.4")
            bundle("commons", listOf("commons-cli", "commons-beanutils"))

            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")

            // do not include these in jars that are distributed - these
            // should only be used when the distributed artifact is deployable (e.g.
            // a docker image)
            library("slf4j-log4j2", "org.apache.logging.log4j", "log4j-slf4j-impl").versionRef("log4j2")
            library("log4j-core", "org.apache.logging.log4j", "log4j-core").versionRef("log4j2")
            bundle("logging", listOf("slf4j-log4j2", "log4j-core"))
        }

        create("testlibs") {
            version("testcontainers", "1.17.6")

            library("junit", "org.junit.jupiter:junit-jupiter:5.9.1")
            library("hamcrest", "org.hamcrest:hamcrest:2.2")
            library("mockito", "org.mockito:mockito-core:5.2.0")
            library("mockito-jupiter", "org.mockito:mockito-junit-jupiter:5.3.1")

            // include these as test dependencies so we don't force downstream
            // users to use our logging libraries
            library("slf4j", "org.slf4j:slf4j-log4j12:1.7.5")
            library("log4j-core", "org.apache.logging.log4j:log4j-core:2.17.1")

            library("kafka-streams-test-utils", "org.apache.kafka:kafka-streams-test-utils:3.4.0")
            library("kafka-clients", "org.apache.kafka:kafka-clients:3.4.0")

            library("testcontainers", "org.testcontainers", "testcontainers").versionRef("testcontainers")
            library("testcontainers-junit", "org.testcontainers", "junit-jupiter").versionRef("testcontainers")
            library("testcontainers-cassandra", "org.testcontainers", "cassandra").versionRef("testcontainers")
            library("testcontainers-kafka", "org.testcontainers", "kafka").versionRef("testcontainers")
            bundle("testcontainers", listOf("testcontainers", "testcontainers-junit", "testcontainers-cassandra", "testcontainers-kafka"))

            bundle("base", listOf("junit", "slf4j", "log4j-core", "hamcrest", "mockito", "mockito-jupiter"))
        }
    }
}

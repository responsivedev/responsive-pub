/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

rootProject.name = "responsive-pub"

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
    id("com.gradle.enterprise") version "3.13.4"
}

include("kafka-client")
include("kafka-client-examples:simple-example")
include("kafka-client-examples:e2e-test")
include("kafka-client-bootstrap")
include("responsive-test-utils")

include("controller-api")
include("operator")
include("tools")

gradleEnterprise {
    if (System.getenv("CI") != null) {
        buildScan {
            publishAlways()
            termsOfServiceUrl = "https://gradle.com/terms-of-service"
            termsOfServiceAgree = "yes"
        }
    }
}

dependencyResolutionManagement {

    versionCatalogs {
        create("libs") {
            version("jackson", "2.15.2")
            version("kafka", "4.0.0")
            version("scylla", "4.15.0.0")
            version("javaoperatorsdk", "4.9.6")
            version("grpc", "1.69.1")
            version("protobuf-java", "3.25.5")
            version("slf4j", "1.7.5")
            version("log4j2", "2.20.0")
            version("mongoDB", "4.10.2")
            version("fabric8", "6.13.4")

            library("jackson", "com.fasterxml.jackson.datatype", "jackson-datatype-jdk8").versionRef("jackson")

            library("kafka-clients", "org.apache.kafka", "kafka-clients").versionRef("kafka")
            library("kafka-streams", "org.apache.kafka", "kafka-streams").versionRef("kafka")
            library("kafka-streams-test-utils", "org.apache.kafka", "kafka-streams-test-utils").versionRef("kafka")

            library("scylla-driver-core", "com.scylladb", "java-driver-core").versionRef("scylla")
            library("scylla-query-builder", "com.scylladb", "java-driver-query-builder").versionRef("scylla")
            library("scylla-mapper-runtime", "com.scylladb", "java-driver-mapper-runtime").versionRef("scylla")
            bundle("scylla", listOf("scylla-driver-core", "scylla-query-builder", "scylla-mapper-runtime"))

            library("mongodb-driver-core", "org.mongodb", "mongodb-driver-core").versionRef("mongoDB")
            library("mongodb-driver-sync", "org.mongodb", "mongodb-driver-sync").versionRef("mongoDB")

            library("javaoperatorsdk", "io.javaoperatorsdk", "operator-framework").versionRef("javaoperatorsdk")

            library("grpc-netty", "io.grpc", "grpc-netty").versionRef("grpc")
            library("grpc-protobuf", "io.grpc", "grpc-protobuf").versionRef("grpc")
            library("grpc-stub", "io.grpc", "grpc-stub").versionRef("grpc")
            // Workaround for @javax.annotation.Generated
            // see: https://github.com/grpc/grpc-java/issues/3633
            library("javax-annotation-api", "javax.annotation", "javax.annotation-api").version("1.3.1")
            bundle("grpc", listOf("grpc-netty", "grpc-protobuf", "grpc-stub", "javax-annotation-api"))

            library("protobuf-java-util", "com.google.protobuf", "protobuf-java-util").versionRef("protobuf-java")
            library("crd-generator-atp", "io.fabric8", "crd-generator-apt").versionRef("fabric8")

            library("guava", "com.google.guava:guava:32.1.3-jre")

            library("commons-cli", "commons-cli:commons-cli:1.5.0")
            library("commons-beanutils", "commons-beanutils:commons-beanutils:1.9.4")
            library("commons-codec", "commons-codec:commons-codec:1.17.0")
            bundle("commons", listOf("commons-cli", "commons-beanutils", "commons-codec"))

            library("slf4j-api", "org.slf4j", "slf4j-api").versionRef("slf4j")

            library("otel-api", "io.opentelemetry:opentelemetry-api:1.32.0");
            library("otel-sdk", "io.opentelemetry:opentelemetry-sdk:1.32.0");
            library("otel-sdk-metrics", "io.opentelemetry:opentelemetry-sdk-metrics:1.32.0");
            library("otel-exporter-logging", "io.opentelemetry:opentelemetry-exporter-logging:1.32.0");
            library("otel-exporter-otlp", "io.opentelemetry:opentelemetry-exporter-otlp:1.32.0");
            library("otel-jmx", "io.opentelemetry.instrumentation:opentelemetry-jmx-metrics:1.32.0-alpha")
            library("otel-resource", "io.opentelemetry.instrumentation:opentelemetry-resources:1.32.0-alpha")
            bundle("otel", listOf(
                    "otel-api",
                    "otel-sdk",
                    "otel-sdk-metrics",
                    "otel-exporter-logging",
                    "otel-exporter-otlp",
                    "otel-jmx",
                    "otel-resource"
            ))

            // do not include these in jars that are distributed - these
            // should only be used when the distributed artifact is deployable (e.g.
            // a docker image)
            library("slf4j-log4j2", "org.apache.logging.log4j", "log4j-slf4j-impl").versionRef("log4j2")
            library("log4j-core", "org.apache.logging.log4j", "log4j-core").versionRef("log4j2")
            bundle("logging", listOf("slf4j-log4j2", "log4j-core"))
        }

        create("testlibs") {
            version("testcontainers", "1.17.6")
            version("grpc", "1.69.1")

            library("junit", "org.junit.jupiter:junit-jupiter:5.9.1")
            library("hamcrest", "org.hamcrest:hamcrest:2.2")
            library("mockito", "org.mockito:mockito-core:5.2.0")
            library("mockito-jupiter", "org.mockito:mockito-junit-jupiter:5.3.1")

            // include these as test dependencies so we don't force downstream
            // users to use our logging libraries
            library("slf4j", "org.slf4j:slf4j-log4j12:1.7.5")
            library("log4j-core", "org.apache.logging.log4j:log4j-core:2.17.1")

            library("testcontainers", "org.testcontainers", "testcontainers").versionRef("testcontainers")
            library("testcontainers-junit", "org.testcontainers", "junit-jupiter").versionRef("testcontainers")
            library("testcontainers-cassandra", "org.testcontainers", "cassandra").versionRef("testcontainers")
            library("testcontainers-kafka", "org.testcontainers", "kafka").versionRef("testcontainers")
            library("testcontainers-mongodb", "org.testcontainers", "mongodb").versionRef("testcontainers")
            library("testcontainers-k3", "org.testcontainers", "k3s").versionRef("testcontainers")
            bundle("testcontainers",
                    listOf(
                            "testcontainers",
                            "testcontainers-junit",
                            "testcontainers-cassandra",
                            "testcontainers-kafka",
                            "testcontainers-mongodb",
                            "testcontainers-k3",
                    ))

            bundle("base", listOf("junit", "slf4j", "log4j-core", "hamcrest", "mockito", "mockito-jupiter"))

            library("grpc-inprocess", "io.grpc", "grpc-inprocess").versionRef("grpc")
            library("grpc-testing", "io.grpc", "grpc-testing").versionRef("grpc")
            library("grpc-testing-proto", "io.grpc", "grpc-testing-proto").versionRef("grpc")
            bundle("grpctesting", listOf("grpc-inprocess", "grpc-testing", "grpc-testing-proto"))
        }
    }
}

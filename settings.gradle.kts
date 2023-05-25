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

include("kafka-client")

dependencyResolutionManagement {

    versionCatalogs {
        create("libs") {
            version("kafka", "3.4.0")
            version("scylla", "4.15.0.0")

            library("kafka-streams", "org.apache.kafka", "kafka-streams").versionRef("kafka")

            library("scylla-driver-core", "com.scylladb", "java-driver-core").versionRef("scylla")
            library("scylla-query-builder", "com.scylladb", "java-driver-query-builder").versionRef("scylla")
            library("scylla-mapper-runtime", "com.scylladb", "java-driver-mapper-runtime").versionRef("scylla")

            bundle("scylla", listOf("scylla-driver-core", "scylla-query-builder", "scylla-mapper-runtime"))
        }

        create("testlibs") {
            version("testcontainers", "1.17.6")

            library("junit", "junit:junit:4.11")
            library("hamcrest", "org.hamcrest:hamcrest:2.2")
            library("mockito", "org.mockito:mockito-core:5.2.0")

            library("kafka-streams-test-utils", "org.apache.kafka:kafka-streams-test-utils:3.4.0")

            library("testcontainers", "org.testcontainers", "testcontainers").versionRef("testcontainers")
            library("testcontainers-cassandra", "org.testcontainers", "cassandra").versionRef("testcontainers")
            library("testcontainers-kafka", "org.testcontainers", "kafka").versionRef("testcontainers")

            bundle("base", listOf("junit", "hamcrest", "mockito"))
        }
    }
}

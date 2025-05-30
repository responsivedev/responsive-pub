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

import java.io.ByteArrayOutputStream

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

import com.google.protobuf.gradle.id

buildscript {
    dependencies {
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.9.4")
    }
}

plugins {
    id("responsive.java-library-conventions")
    id("java")
    id("com.google.protobuf").version("0.9.4")
}

sourceSets {
    main {
        proto {
            srcDir("src/main/external-protos/rs3/proto")
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.5"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.orNull}"
        }
    }

    generateProtoTasks {
        all().forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without
                // options. Note the braces cannot be omitted, otherwise the
                // plugin will not be added. This is because of the implicit way
                // NamedDomainObjectContainer binds the methods.
                id("grpc") {
                }
            }
        }
    }
}

/*********** Generated Resources ***********/

val gitCommitId: String by lazy {
    val stdout = ByteArrayOutputStream()
    //rootProject.exec {
    exec {
        commandLine("git", "rev-parse", "--verify", "--short", "HEAD")
        standardOutput = stdout
    }
    stdout.toString().trim()
}

val writeVersionPropertiesFile = "writeVersionPropertiesFile"
val gitVersion = version

val resourcesDir = "$buildDir/resources/main"
val versionFilePath = "$resourcesDir/version.properties"

tasks.register(writeVersionPropertiesFile) {
    val versionFile = file(versionFilePath)
    outputs.file(versionFile)
    doFirst {
        file(versionFilePath).writeText(
                "git.build.version=" + gitVersion + "\n" +
                "git.commit.id=" + gitCommitId + "\n"
        )
    }
}

tasks.compileJava {
    dependsOn(tasks[writeVersionPropertiesFile])
}

tasks.publishToMavenLocal {
    dependsOn(tasks[writeVersionPropertiesFile])
}

tasks.publish {
    dependsOn(tasks[writeVersionPropertiesFile])
}

configurations {
    create("testArtifacts")
}

tasks.register<Jar>("testJar") {
    from(sourceSets["test"].output)
    archiveClassifier.set("test")
}

artifacts {
    add("testArtifacts", tasks["testJar"])
}

/********************************************/

dependencies {
    api(libs.kafka.streams)

    implementation("dev.responsive:controller-api:0.16.0")
    implementation(libs.bundles.scylla)
    implementation(libs.bundles.commons)
    implementation(libs.jackson)
    implementation(libs.mongodb.driver.sync)
    implementation(libs.bundles.otel)
    implementation(libs.bundles.grpc)
    implementation(libs.protobuf.java.util)
    implementation(libs.guava)

    testImplementation(libs.kafka.clients) {
        artifact {
            classifier = "test"
        }
    }
    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.bundles.testcontainers)
    testImplementation(libs.kafka.streams.test.utils)
    testImplementation("io.grpc:grpc-inprocess:${libs.versions.grpc.orNull}")
    testImplementation("software.amazon.awssdk:kms:2.20.0")
    testImplementation("software.amazon.awssdk:sso:2.20.0")
    testImplementation("net.jqwik:jqwik:1.9.2")
}

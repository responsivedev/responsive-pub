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

plugins {
    id("responsive.java-library-conventions")
    id("java")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
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

dependencies {
    api(project(":kafka-client"))
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("org.springframework.kafka:spring-kafka:3.2.4")
    implementation("org.springframework.boot:spring-boot-starter:3.3.2")

    testImplementation(testlibs.bundles.base)
}
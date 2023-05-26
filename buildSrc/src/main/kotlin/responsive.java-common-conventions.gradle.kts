import gradle.kotlin.dsl.accessors._d9de297972d0a4752442781be40b0b07.java

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
    `java`
    `checkstyle`
}

dependencies {
    checkstyle("com.puppycrawl.tools:checkstyle:10.11.0")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

checkstyle {
    version = "10.11.0"
    maxWarnings = 0
}

repositories {
    mavenCentral()
}

tasks.test {
    // Use the built-in JUnit support of Gradle.
    useJUnitPlatform()
}
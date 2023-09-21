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
    id("com.gorylenko.gradle-git-properties") version "2.4.1"
}

configure<com.gorylenko.GitPropertiesPluginExtension> {
    // Creates a file with various git properties at build/resources/main/{gitPropertiesName}
    // See https://github.com/n0mer/gradle-git-properties#usage for all available props

    // Can be a file name or relative path under build/resources/main
    gitPropertiesName = "version.properties"

    keys = arrayOf("git.build.version","git.commit.id.abbrev").toMutableList()
}


dependencies {
    api(libs.kafka.streams)

    implementation(libs.bundles.scylla)

    testImplementation(libs.kafka.clients) {
        artifact {
            classifier = "test"
        }
    }
    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.bundles.testcontainers)
    testImplementation(libs.kafka.streams.test.utils)
}
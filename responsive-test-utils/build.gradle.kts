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
}

dependencies {
    implementation(project(":kafka-client"))
    implementation(libs.kafka.streams.test.utils)
    implementation(libs.kafka.clients) {
        artifact {
            classifier = "test"
        }
    }

    testImplementation(testlibs.bundles.base)
    testImplementation(libs.bundles.logging)
    testImplementation(testlibs.kafka.streams.test.utils)
}
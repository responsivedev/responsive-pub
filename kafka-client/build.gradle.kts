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
    implementation(libs.kafka.streams)
    implementation(libs.bundles.scylla)

    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.kafka.streams.test.utils)
    testImplementation(testlibs.testcontainers)
    testImplementation(testlibs.testcontainers.cassandra)
    testImplementation(testlibs.testcontainers.kafka)
}
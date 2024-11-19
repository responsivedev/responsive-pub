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

plugins {
    id("responsive.java-library-conventions")
}

repositories {
    mavenLocal()
}

version = project(":kafka-client").version

dependencies {
    implementation(project(":kafka-client"))
    api(libs.kafka.streams.test.utils)

    implementation(libs.bundles.scylla)

    implementation(variantOf(libs.kafka.clients) {
        classifier("test")
    })

    testImplementation(project(":kafka-client", configuration = "testArtifacts"))

    testImplementation(testlibs.bundles.base)
    testImplementation(libs.bundles.logging)
}
/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

plugins {
    id("responsive.java-application-conventions")
    id("responsive.docker")
}

application {
    mainClass.set("dev.responsive.kafka.bootstrap.main.Main")
}

dependencies {
    implementation(project(":kafka-client"))
    implementation(libs.bundles.commons)
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

version = project(":kafka-client").version

responsive_docker.dockerImage.set("kafka-client-bootstrap:$version")

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
    id("responsive.java-application-conventions")
    id("responsive.docker")
    id("responsive.helm")
}

application {
    mainClass.set("dev.responsive.k8s.operator.OperatorMain")
}

dependencies {
    implementation(project(":controller-api"))

    implementation(libs.crd.generator.atp)
    implementation(libs.jackson)
    implementation(libs.javaoperatorsdk)

    implementation(libs.bundles.commons)
    implementation(libs.bundles.logging)

    implementation(libs.kafka.clients)

    annotationProcessor(libs.javaoperatorsdk)
    annotationProcessor(libs.crd.generator.atp)

    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.bundles.testcontainers)
    testImplementation("io.grpc:grpc-testing:1.53.0")
    testImplementation("io.grpc:grpc-examples:0.15.0")

    testImplementation("org.bouncycastle:bcprov-jdk15on:1.70")
    testImplementation("org.bouncycastle:bcpkix-jdk15on:1.70")
}

responsive_docker.dockerImage.set("responsive-operator:$version")

tasks {
    register("pushCRD", Exec::class) {
        commandLine("aws", "s3", "cp", "$buildDir/classes/java/main/META-INF/fabric8/responsivepolicies.application.responsive.dev-v1.yml", "s3://crd.responsive.dev/responsive-operator/revisions/$version/crd.yml")
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.remove("-Werror")
}

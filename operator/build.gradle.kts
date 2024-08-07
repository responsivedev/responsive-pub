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

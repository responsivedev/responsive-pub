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
}

application {
    mainClass.set("dev.responsive.k8s.operator.OperatorMain")
}

dependencies {
    implementation(project(":controller-api"))

    implementation(libs.crd.generator.atp)
    implementation(libs.jackson)
    implementation(libs.javaoperatorsdk)

    annotationProcessor(libs.javaoperatorsdk)
    annotationProcessor(libs.crd.generator.atp)

    testImplementation(testlibs.bundles.base)
}
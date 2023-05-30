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

// TODO(rohan): figure out how to put these somewhere common
//              and move the defs to properties at top level

val dockerImage = "responsive-operator:" + version
var dockerRepoBase = "292505934682.dkr.ecr.us-west-2.amazonaws.com/responsiveinc/"
if (project.hasProperty("dockerRegistry")) {
    val dockerRegistry = project.property("dockerRegistry") as String
    dockerRepoBase = dockerRegistry + "/responsiveinc/"
}
var helmRegistry = "public.ecr.aws/j8q9y0n6"
if (project.hasProperty("helmRegistry")) {
    helmRegistry = project.property("helmRegistry") as String
}

tasks {
    register("copyJars", Copy::class) {
        dependsOn("build")
        into("$buildDir/docker/libs")
        from(configurations.runtimeClasspath)
        from("$buildDir/libs")
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }

    register("copyDockerDir", Copy::class) {
        into("$buildDir/docker")
        from("$projectDir/docker")
        include("**/*")
    }

    register("buildDocker", Exec::class) {
        dependsOn("pushCRD")
        dependsOn("copyJars")
        dependsOn("copyDockerDir")
        workingDir("$buildDir")
        commandLine("docker", "build", "--platform",  "linux/amd64", "-t", "$dockerImage", "docker")
    }

    register("tagDocker", Exec::class) {
        dependsOn("buildDocker")
        commandLine("docker", "tag", "$dockerImage", "$dockerRepoBase$dockerImage")
    }

    register("pushCRD", Exec::class) {
        commandLine("aws", "s3", "cp", "$buildDir/classes/java/main/META-INF/fabric8/responsivepolicies.application.responsive.dev-v1.yml", "s3://crds.responsive.dev/responsive-operator/revisions/$version/crd.yml")
    }

    register("pushDocker", Exec::class) {
        dependsOn("tagDocker")
        commandLine("docker", "push", "$dockerRepoBase$dockerImage")
    }

    register("loadDockerKind", Exec::class) {
        dependsOn("buildDocker")
        commandLine("kind", "load", "docker-image", "$dockerImage")
    }

    register("packageHelm", Exec::class) {
        doFirst {
            mkdir("$buildDir/helm")
        }
        workingDir("$buildDir/helm")
        commandLine("helm", "package", "--app-version", version, "--version", version, "$projectDir/src/main/helm/")
    }

    register("pushHelm", Exec::class) {
        dependsOn("packageHelm")
        workingDir("$buildDir/helm")
        commandLine("helm", "push", "responsive-operator-chart-$version.tgz", "oci://$helmRegistry")
    }
}

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

import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.register

interface DockerPluginExtension {
    val dockerImage: Property<String>
    val dockerRepoBase: Property<String>
}

class DockerPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val config = project.extensions.create<DockerPluginExtension>("responsive_docker")

        var dockerRepoBase: String
        if (!config.dockerRepoBase.isPresent()) {
            if (project.hasProperty("dockerRegistry")) {
                val dockerRegistry = project.property("dockerRegistry") as String
                dockerRepoBase = dockerRegistry + "/responsiveinc/"
            } else {
                dockerRepoBase = "public.ecr.aws/j8q9y0n6/responsiveinc/"
            }
        } else {
            dockerRepoBase = config.dockerRepoBase.get()
        }

        project.tasks.register("copyJars", Copy::class) {
            dependsOn("build")
            into(project.buildDir.getPath() + "/docker/libs")
            from(project.getConfigurations().getByName("runtimeClasspath"))
            from(project.buildDir.getPath() + "/libs")
            duplicatesStrategy = DuplicatesStrategy.INCLUDE
        }

        project.tasks.register("copyDockerDir", Copy::class) {
            into(project.buildDir.getPath() + "/docker")
            from(project.projectDir.getPath() + "/docker")
            include("**/*")
        }

        project.tasks.register("buildDocker", Exec::class) {
            dependsOn("copyJars")
            dependsOn("copyDockerDir")
            workingDir(project.buildDir.getPath())
            commandLine(
                    "docker",
                    "build",
                    "--platform",
                    "linux/amd64",
                    "-t",
                    config.dockerImage.get(),
                    "docker"
            )
        }

        project.tasks.register("loadDockerKind", Exec::class) {
            dependsOn("buildDocker")
            commandLine("kind", "load", "docker-image", config.dockerImage.get())
        }

        project.tasks.register("pushDockerMultiArch", Exec::class) {
            dependsOn("copyJars")
            dependsOn("copyDockerDir")
            workingDir(project.buildDir.getPath())
            commandLine(
                    "docker",
                    "buildx",
                    "build",
                    "--platform",
                    "linux/amd64,linux/arm64",
                    "-t",
                    dockerRepoBase + config.dockerImage.get(),
                    "--push",
                    "docker"
            )
        }
    }
}

apply<DockerPlugin>()

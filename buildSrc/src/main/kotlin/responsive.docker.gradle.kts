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

        project.tasks.getByName("build") {
            mustRunAfter("clean")
        }

        project.tasks.register("copyJars", Copy::class) {
            dependsOn("clean", "build")
            into(project.buildDir.getPath() + "/docker/libs")
            from(project.getConfigurations().getByName("runtimeClasspath"))
            from(project.buildDir.getPath() + "/libs")
            duplicatesStrategy = DuplicatesStrategy.INCLUDE
        }

        project.tasks.register("copyDockerDir", Copy::class) {
            dependsOn("clean")
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

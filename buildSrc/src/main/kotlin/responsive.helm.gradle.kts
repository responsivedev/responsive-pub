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

import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.register

interface HelmPluginExtension {
    val helmRegistry: Property<String>
}

class HelmPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val config = project.extensions.create<HelmPluginExtension>("responsive_helm")

        var helmRegistry: String
        if (config.helmRegistry.isPresent()) {
            helmRegistry = config.helmRegistry.get()
        } else {
            if (project.hasProperty("helmRegistry")) {
                helmRegistry = project.property("helmRegistry") as String
            } else {
                helmRegistry = "public.ecr.aws/j8q9y0n6"
            }
        }

        val version = project.version

        project.tasks.register("makeHelmDir", Exec::class) {
            commandLine("mkdir", "-p", project.buildDir.getPath() + "/helm")
        }

        project.tasks.register("packageHelm", Exec::class) {
            dependsOn("makeHelmDir")
            workingDir(project.buildDir.getPath() + "/helm")
            commandLine(
                    "helm",
                    "package",
                    "--app-version",
                    version,
                    "--version",
                    version,
                    project.projectDir.getPath() + "/src/main/helm/"
            )
        }

        project.tasks.register("pushHelm", Exec::class) {
            dependsOn("packageHelm")
            workingDir(project.buildDir.getPath() + "/helm")
            commandLine(
                    "helm",
                    "push",
                    "responsive-operator-" + version + ".tgz",
                    "oci://" + helmRegistry + "/responsiveinc/charts"
            )
        }
    }
}

apply<HelmPlugin>()
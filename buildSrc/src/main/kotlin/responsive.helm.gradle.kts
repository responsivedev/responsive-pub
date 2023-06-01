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
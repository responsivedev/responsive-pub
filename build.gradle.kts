import java.util.regex.Matcher

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
    id("net.researchgate.release") version "3.0.2"
}

release {
    tagTemplate.set("v${ version.toString().dropLast("-SNAPSHOT".length) }")
    failOnPublishNeeded.set(false)
    failOnUpdateNeeded.set(false)

    val versionRegex = "(?<major>\\d+).(?<minor>\\d+).(?<patch>\\d+)(?<ext>[^\\d]*$)"
    versionPatterns = mapOf(versionRegex to KotlinClosure2<Matcher, Project, String>({
        m, p -> String.format(
            "%s.%s.0%s",
            m.group("major"),
            Integer.parseInt(m.group("minor")) + 1,
            m.group("ext"))
    }))

    with(git) {
        requireBranch.set("main")
    }
}
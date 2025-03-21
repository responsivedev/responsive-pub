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

import com.adarshr.gradle.testlogger.theme.ThemeType

plugins {
    `java`
    `checkstyle`
    `maven-publish`
    `signing`

    id("pl.allegro.tech.build.axion-release")
    id("com.adarshr.test-logger")

}

dependencies {
    checkstyle("com.puppycrawl.tools:checkstyle:10.11.0")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

checkstyle {
    version = "10.11.0"
    maxWarnings = 0
}

repositories {
    mavenCentral()
}

tasks.test {
    // Use the built-in JUnit support of Gradle.
    useJUnitPlatform()
}

testlogger {
    theme = ThemeType.MOCHA
}

allprojects {

    scmVersion {

        tag {
            prefix.set(project.name)
            versionSeparator.set("-")
            initialVersion({_, _ -> "0.1.0"})
        }

        versionIncrementer("incrementMinor")

        branchVersionIncrementer.putAll( mapOf<String, String>(
            "patch/.*" to "incrementPatch"
        ))

        snapshotCreator({ _, _ -> "-SNAPSHOT" })
        ignoreUncommittedChanges.set(false)
    }

    group = "dev.responsive"
    version = scmVersion.version

    java {
        withJavadocJar()
        withSourcesJar()
    }

    publishing {
        publications {
            create<MavenPublication>("mavenJava") {
                artifactId = project.name
                from(components["java"])

                pom {
                    name.set(project.name)
                    description.set("artifact for " + project.name)
                    url.set("https://responsive.dev")

                    licenses {
                        license {
                            name.set("Responsive Business Source License v1.0")
                            url.set("https://www.responsive.dev/legal/responsive-bsl-10")
                        }
                    }

                    developers {
                        developer {
                            name.set("Responsive Tools Team")
                            email.set("tools@responsive.dev")
                            organization.set("Responsive Computing, Inc")
                            organizationUrl.set("https://www.responsive.dev")
                        }
                    }

                    scm {
                        connection.set("scm:git:git://github.com/responsivedev/responsive-pub.git")
                        developerConnection.set("scm:git:ssh://github.com:responsivedev/responsive-pub.git")
                        url.set("http://github.com/responsivedev/responsive-pub/tree/main")
                    }
                }
            }
        }

        repositories {
            maven {
                name = "s3"
                val releasesUrl = "s3://maven-repo.responsive.dev/releases"
                val snapshotsUrl = "s3://maven-repo.responsive.dev/snapshots"
                url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsUrl else releasesUrl)
                authentication { create<AwsImAuthentication>("awsIm") }
            }

            maven {
                name = "ossrh"
                val releasesUrl = "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
                val snapshotsUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsUrl else releasesUrl)
                credentials {
                    username = System.getenv("OSSRH_USER") ?: return@credentials
                    password = System.getenv("OSSRH_PASSWORD") ?: return@credentials
                }
            }
        }

        signing {
            val key = System.getenv("SIGNING_KEY") ?: return@signing
            val password = System.getenv("SIGNING_PASSWORD") ?: return@signing
            useInMemoryPgpKeys(key, password)

            sign(publishing.publications["mavenJava"])
        }
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.withType<Javadoc> {
    options {
        (this as CoreJavadocOptions).addStringOption("Xdoclint:none", "-quiet")
    }
}

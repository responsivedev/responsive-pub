plugins {
    id("responsive.java-application-conventions")
}

application {
    mainClass.set("dev.responsive.examples.async.Main")
}

dependencies {
    // todo: how to set the version here?
    implementation(project(":kafka-client"))
    implementation("com.google.guava:guava:32.1.1-jre")
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("org.apache.kafka:kafka-streams:3.7.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation(platform("software.amazon.awssdk:bom:2.27.2"))
    implementation("software.amazon.awssdk:dynamodb:2.27.2")
    implementation("com.github.seratch:jslack:3.4.2")
    implementation("com.scylladb:java-driver-core:4.15.0.0")
    implementation("software.amazon.awssdk:sso:2.27.2")
    implementation("com.scylladb:java-driver-query-builder:4.15.0.0")
    implementation("com.scylladb:java-driver-mapper-runtime:4.15.0.0")
    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.bundles.testcontainers)
}

/*
buildscript {
    dependencies {
        // required for AWS SSO authentication to work
        classpath("software.amazon.awssdk:sso:2.27.2")
    }
}
*/

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

fun String.runCommand(wd: File = file("./")): String {
    val byteOut = `java.io`.ByteArrayOutputStream()
    project.exec {
        workingDir = wd
        commandLine = this@runCommand.split("\\s".toRegex())
        standardOutput = byteOut
    }
    return String(byteOut.toByteArray()).trim()
}

val kc_version = project(":kafka-client").version
version = "$kc_version" + "-" + "git rev-parse --short HEAD".runCommand(wd = rootDir)

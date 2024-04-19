plugins {
    id("responsive.java-application-conventions")
    id("responsive.docker")
}

application {
    mainClass.set("dev.responsive.examples.e2etest.Main")
}

dependencies {
    // todo: how to set the version here?
    implementation(project(":kafka-client"))
    implementation("com.google.guava:guava:32.1.1-jre")
    implementation("org.apache.kafka:kafka-clients:3.4.0")
    implementation("org.apache.kafka:kafka-streams:3.4.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("com.scylladb:java-driver-core:4.15.0.0")
    implementation("com.scylladb:java-driver-query-builder:4.15.0.0")
    implementation("com.scylladb:java-driver-mapper-runtime:4.15.0.0")
    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.bundles.testcontainers)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

version = project(":kafka-client").version

responsive_docker.dockerImage.set("e2e-test:$version")

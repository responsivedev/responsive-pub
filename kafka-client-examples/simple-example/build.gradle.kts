plugins {
    id("responsive.java-application-conventions")
    id("responsive.docker")
}

application {
    mainClass.set("dev.responsive.examples.simpleapp.Main")
}

dependencies {
    // todo: how to set the version here?
    implementation(project(":kafka-client"))
    implementation("com.google.guava:guava:32.1.1-jre")
    implementation("org.apache.kafka:kafka-clients:3.4.0")
    implementation("org.apache.kafka:kafka-streams:3.4.0")
    implementation("io.opentelemetry.javaagent:opentelemetry-javaagent:1.25.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("com.scylladb:java-driver-core:4.15.0.0")
    implementation("com.scylladb:java-driver-query-builder:4.15.0.0")
    implementation("com.scylladb:java-driver-mapper-runtime:4.15.0.0")
    implementation(libs.bundles.otel)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

version = project(":kafka-client").version

responsive_docker.dockerImage.set("simple-example:$version")

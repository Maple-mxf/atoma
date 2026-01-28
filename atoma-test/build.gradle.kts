plugins {
    id("java")
    id("java-library")
}

group = "atoma.test"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    api(project(":atoma-api"))
    api(project(":atoma-core"))
    api(project(":atoma-client"))
    api(project(":atoma-storage-mongo"))
    testImplementation(lib.guava)
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("junit:junit:4.13.2")
    testImplementation(lib.systemrule)
    testImplementation(lib.mongodriver)
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.11.0")
    testImplementation(lib.flapdoodle.embed.mongo)
    testImplementation("org.testcontainers:testcontainers:1.19.7")
    testImplementation("org.testcontainers:junit-jupiter:1.19.7")
    testImplementation(lib.testcontainers.mongodb)
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.awaitility:awaitility:4.3.0")
    testImplementation("org.junit.platform:junit-platform-suite-api:6.0.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
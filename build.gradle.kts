plugins {
    id("java")
}

group = "atoma"
version = "1.0"

allprojects {
    apply(plugin = "java")
    repositories {
        mavenCentral()
    }
    dependencies {
        testImplementation("de.flapdoodle.embed:de.flapdoodle.embed.mongo:4.16.1")
    }
}


tasks.test {
    useJUnitPlatform()
}
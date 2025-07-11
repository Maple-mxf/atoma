plugins {
    id("java")
}

group = "atoma"
version = "1.0"

allprojects {
    repositories {
        mavenCentral()
    }
}


tasks.test {
    useJUnitPlatform()
}
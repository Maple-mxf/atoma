plugins {
    id("java")
}

group = "atoma.api"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Javadoc> {
    options {
        this as StandardJavadocDocletOptions
        encoding = "UTF-8"
        links("https://docs.oracle.com/javase/8/docs/api/")

        if (JavaVersion.current().isJava9Compatible) {
            addBooleanOption("html5", true)
        }

        tags = listOf(
            "apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:"
        )
    }

    title = "Atoma Project API Documentation"
}
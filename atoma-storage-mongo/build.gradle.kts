import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("java")
    id("java-library")
    id("net.ltgt.errorprone") version "4.1.0"
}

group = "atoma.storage.mongo"
version = "1.0"

dependencies {
    api(project(":atoma-api"))
    implementation(lib.guava)
    implementation(lib.mongodriver)
    implementation(lib.failsafe)
    implementation(lib.slf4j)
    errorprone("com.google.errorprone:error_prone_core:2.28.0")


    runtimeOnly(lib.logback)

    compileOnly(lib.autoserviceannotations)
    compileOnly(lib.autovalueannotations)
    annotationProcessor(lib.autoservice)
    annotationProcessor(lib.autovalue)

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("junit:junit:4.13.2")
    testImplementation(lib.systemrule)
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.11.0")
    testImplementation(lib.flapdoodle.embed.mongo)
}

tasks.withType<Javadoc> {
    options {
        this as StandardJavadocDocletOptions
        encoding = "UTF-8"
        links("https://docs.oracle.com/javase/8/docs/api/")

        if (JavaVersion.current().isJava9Compatible) {
            addBooleanOption("html5", true)
        }
        addStringOption("Xdoclint:none", "-quiet")
        tags = listOf(
            "apiNote:a:API Note:", "implSpec:a:Implementation Requirements:", "implNote:a:Implementation Note:"
        )
    }

    title = "Atoma Project API Documentation"
}

tasks.withType<JavaCompile>().configureEach {
    options.errorprone.disableWarningsInGeneratedCode.set(true)
    options.errorprone.disableAllChecks=true
}

tasks.test {
    useJUnitPlatform()
}

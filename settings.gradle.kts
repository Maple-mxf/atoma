rootProject.name = "atoma"

dependencyResolutionManagement {
    versionCatalogs {
        create("lib") {
            from(files("libs.versions.toml"))
        }
    }
}
include("api")
include("core")

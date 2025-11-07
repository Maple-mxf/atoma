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
include("atoma-api")
include("atoma-storage-mongo")
include("atoma-core")
include("atoma-client")

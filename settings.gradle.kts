rootProject.name = "atoma"

dependencyResolutionManagement {
    versionCatalogs {
        create("lib") {
            from(files("libs.versions.toml"))
        }
    }
}
include("atoma-api")
include("atoma-storage-mongo")
include("atoma-core")
include("atoma-client")
include("atoma-benchmark")

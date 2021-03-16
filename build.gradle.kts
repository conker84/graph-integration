plugins {
    kotlin("multiplatform") version "1.4.30"
    kotlin("plugin.serialization") version "1.4.30"
    `maven-publish`
}

group = "org.neo4j"
version = "1.0-SNAPSHOT-4"

repositories {
    mavenCentral()
}

val isJitPack = System.getenv("JITPACK")?.toBoolean() ?: false
println("isJitPack: $isJitPack")

kotlin {
    jvm {
        compilations.all {
            kotlinOptions.jvmTarget = "1.8"
        }
        testRuns["test"].executionTask.configure {
            useJUnit()
        }
    }
    if (!isJitPack) {
        js(IR) { // we try to use the IR instead LEGACY as it's able to perform aggressive optimizations and other things that were difficult with the LEGACY compiler
            browser {
                testTask {
                    useKarma {
                        useChromeHeadless()
                    }
                }
            }
            nodejs {
                testTask {
                    useKarma {
                        useChromeHeadless()
                    }
                }
            }
            useCommonJs()
            binaries.executable()
        }
    }

    sourceSets {
        val commonMain by getting {
//            dependencies {
//                implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.1.0")
//            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
            }
        }
        val jvmMain by getting
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test-junit"))
            }
        }
        if (!isJitPack) {
            val jsMain by getting
            val jsTest by getting {
                dependencies {
                    implementation(kotlin("test-js"))
                }
            }
        }
    }
}

if (isJitPack) {
    val emptyJavadocJar = tasks.register<Jar>("emptyJavadocJar") {
        archiveClassifier.set("javadoc")
    }
    configure<PublishingExtension> {
        publications.withType<MavenPublication>().configureEach {
            artifact(emptyJavadocJar)
            pom
        }
    }
}

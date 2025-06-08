import de.undercouch.gradle.tasks.download.Download
import java.io.File

plugins {
    id("java")
    id("de.undercouch.download") version "5.4.0"
}

group = "org.fleetmap"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    flatDir {
        dirs("libs")
    }
}

dependencies {
    implementation(":tracker-server")
    implementation("org.apache.avro:avro:1.11.4")
    implementation("org.apache.parquet:parquet-avro:1.15.2")
    implementation("org.apache.hadoop:hadoop-common:3.4.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6")
    implementation("software.amazon.awssdk:s3:2.25.17")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

val traccarZip = "https://github.com/traccar/traccar/releases/download/v6.7.2/traccar-other-6.7.2.zip"
val traccarZipFile = File(buildDir, "traccar-other-6.7.2.zip")
val traccarLibDir = file("libs")

tasks.register<Download>("downloadTraccar") {
    src(traccarZip)
    dest(traccarZipFile)
    overwrite(false)
}

tasks.register<Copy>("unzipTraccar") {
    dependsOn("downloadTraccar")
    from(zipTree(traccarZipFile))
    into(traccarLibDir)
}

tasks.named("compileJava") {
    dependsOn("unzipTraccar")
}

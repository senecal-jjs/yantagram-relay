plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

group = "org.yantagram"
version = "2.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.server.status.pages)
    implementation(libs.ktor.server.rate.limit)
    implementation(libs.ktor.server.body.limit)
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.cio)
    implementation(libs.ktor.client.websockets)
    implementation(libs.ktor.serialization.json)
    implementation(libs.coroutines.core)
    implementation(libs.serialization.json)
    implementation(libs.sqlite.jdbc)
    implementation(libs.pushy)
    implementation(libs.google.auth)
    implementation(libs.micrometer.prometheus)
    implementation(libs.logback)
    // Signing the events the migration bridge forwards to strfry (BIP-340).
    implementation(libs.secp256k1)
    implementation(libs.secp256k1.jni)

    testImplementation(kotlin("test"))
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.mock)
    testImplementation(libs.ktor.client.content.negotiation)
    testImplementation(libs.coroutines.test)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("org.yantagram.relay.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

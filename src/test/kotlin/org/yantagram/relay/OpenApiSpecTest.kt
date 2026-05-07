package org.yantagram.relay

import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OpenApiSpecTest {

    private val secret = "test-secret".toByteArray()

    private fun testConfig() = RelayConfig(
        host = "127.0.0.1",
        port = 0,
        publishSecret = secret,
        ringHeapFraction = 0.5,
        ringHardCapBytes = null,
        maxFrameBytes = 1L * 1024 * 1024,
    )

    @Test
    fun `openapi spec is served and valid`() = testApplication {
        val cfg = testConfig()
        application { relayModule(cfg, PacketRing(1024L)) }

        val response = client.get("/openapi.json")
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals(ContentType.Application.Json, response.contentType()?.let { ContentType(it.contentType, it.contentSubtype) })

        val body = response.bodyAsText()
        val json = Json.parseToJsonElement(body).jsonObject

        // Validate OpenAPI version
        assertEquals("3.1.0", json["openapi"]?.jsonPrimitive?.content)

        // Validate info
        val info = json["info"]!!.jsonObject
        assertEquals("Yantagram Relay", info["title"]?.jsonPrimitive?.content)
        assertEquals("1.0.0", info["version"]?.jsonPrimitive?.content)

        // Validate paths exist
        val paths = json["paths"]!!.jsonObject
        assertTrue(paths.containsKey("/publish"), "spec must contain /publish path")
        assertTrue(paths.containsKey("/subscribe"), "spec must contain /subscribe path")

        // Validate /publish has post operation
        val publish = paths["/publish"]!!.jsonObject
        assertTrue(publish.containsKey("post"), "/publish must have POST operation")

        // Validate /subscribe has get operation
        val subscribe = paths["/subscribe"]!!.jsonObject
        assertTrue(subscribe.containsKey("get"), "/subscribe must have GET operation")

        // Validate security scheme exists
        val components = json["components"]!!.jsonObject
        val securitySchemes = components["securitySchemes"]!!.jsonObject
        assertTrue(securitySchemes.containsKey("publishSecret"), "must define publishSecret security scheme")
    }

    @Test
    fun `export openapi spec to build directory`() = testApplication {
        val cfg = testConfig()
        application { relayModule(cfg, PacketRing(1024L)) }

        val response = client.get("/openapi.json")
        assertEquals(HttpStatusCode.OK, response.status)

        val body = response.bodyAsText()

        // Pretty-print for readability
        val prettyJson = Json { prettyPrint = true }
        val parsed = Json.parseToJsonElement(body)
        val formatted = prettyJson.encodeToString(kotlinx.serialization.json.JsonElement.serializer(), parsed)

        val outputDir = File("build/generated/openapi")
        outputDir.mkdirs()
        val outputFile = File(outputDir, "openapi.json")
        outputFile.writeText(formatted)

        assertTrue(outputFile.exists(), "openapi.json should be written to build/generated/openapi/")
        assertTrue(outputFile.length() > 0, "openapi.json should not be empty")
    }
}




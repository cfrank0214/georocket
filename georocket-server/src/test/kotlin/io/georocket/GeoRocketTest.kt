package io.georocket

import com.google.common.collect.Sets
import io.georocket.constants.ConfigConstants
import io.vertx.core.json.JsonObject
import org.junit.Test

import java.util.HashMap

import org.junit.Assert.assertEquals

/**
 * Test [GeoRocket]
 * @author Tim Hellhake
 */
class GeoRocketTest {
    /**
     * Test if the configuration values are overwritten by environment values
     */
    @Test
    fun testOverwriteWithEnvironmentVariables() {
        val PROP_KEY = ConfigConstants.LOG_CONFIG
        val ENV_KEY = PROP_KEY
                .replace(".", "_")
                .toUpperCase()
        val VALUE = "test"
        val conf = JsonObject()
        val env = HashMap<String, String>()
        env[ENV_KEY] = VALUE
        GeoRocket.overwriteWithEnvironmentVariables(conf, env)
        assertEquals(Sets.newHashSet(PROP_KEY), conf.map.keys)
        assertEquals(VALUE, conf.getString(PROP_KEY))
    }
}

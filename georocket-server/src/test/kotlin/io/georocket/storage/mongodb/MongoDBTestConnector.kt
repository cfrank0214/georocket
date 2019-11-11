package io.georocket.storage.mongodb

import java.io.IOException

import com.mongodb.ServerAddress

import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodProcess
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import io.georocket.NetUtils

/**
 * Starts and stops a MongoDB instance
 * @author Michel Kraemer
 */
class MongoDBTestConnector
/**
 * Start MongoDB instance. Don't forget to call [.stop]
 * if you don't need it anymore!
 * @throws IOException if the instance could not be started
 */
@Throws(IOException::class)
constructor() {

    private val mongodExe: MongodExecutable
    private val mongod: MongodProcess

    /**
     * The address of the MongoDB instance
     */
    val serverAddress = ServerAddress("localhost", NetUtils.findPort())

    init {
        mongodExe = starter.prepare(MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(Net(serverAddress.port, Network.localhostIsIPv6()))
                .build())
        mongod = mongodExe.start()
    }

    /**
     * Stop MongoDB instance
     */
    fun stop() {
        mongod.stop()
        mongodExe.stop()
    }

    companion object {
        private val starter = MongodStarter.getDefaultInstance()

        /**
         * The default name of the database to test against
         */
        var MONGODB_DBNAME = "testdb"
    }
}

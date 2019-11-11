package io.georocket.index.elasticsearch

import java.io.IOException
import java.util.HashMap

import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecuteResultHandler
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.ExecuteException
import org.apache.commons.exec.ExecuteWatchdog
import org.apache.commons.exec.Executor
import org.apache.commons.exec.ShutdownHookProcessDestroyer
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang.SystemUtils

import io.georocket.constants.ConfigConstants
import io.georocket.util.RxUtils
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rxjava.core.Vertx
import rx.Completable
import rx.Observable
import rx.Single

/**
 * Runs an Elasticsearch instance
 * @author Michel Kraemer
 */
class ElasticsearchRunner
/**
 * Create the Elasticsearch runner
 * @param vertx the Vert.x instance
 */
(private val vertx: Vertx) {
    private var executor: Executor? = null
    private var stopped: Boolean = false

    /**
     * Run Elasticsearch
     * @param host the host Elasticsearch should bind to
     * @param port the port Elasticsearch should listen on
     * @param elasticsearchInstallPath the path where Elasticsearch is installed
     * @return a Completable that completes when Elasticsearch has started
     */
    fun runElasticsearch(host: String, port: Int,
                         elasticsearchInstallPath: String): Completable {
        val config = vertx.orCreateContext.config()
        val storage = config.getString(ConfigConstants.STORAGE_FILE_PATH)
        val root = "$storage/index"

        return vertx.rxExecuteBlocking<Void> { f ->
            log.info("Starting Elasticsearch ...")

            // get Elasticsearch executable
            var executable = FilenameUtils.separatorsToSystem(
                    elasticsearchInstallPath)
            executable = FilenameUtils.concat(executable, "bin")
            if (SystemUtils.IS_OS_WINDOWS) {
                executable = FilenameUtils.concat(executable, "elasticsearch.bat")
            } else {
                executable = FilenameUtils.concat(executable, "elasticsearch")
            }

            // start Elasticsearch
            val cmdl = CommandLine(executable)
            cmdl.addArgument("-Ecluster.name=georocket-cluster")
            cmdl.addArgument("-Enode.name=georocket-node")
            cmdl.addArgument("-Enetwork.host=$host")
            cmdl.addArgument("-Ehttp.port=$port")
            cmdl.addArgument("-Epath.data=$root/data")

            executor = DefaultExecutor()
            executor!!.processDestroyer = ShutdownHookProcessDestroyer()
            executor!!.watchdog = ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT)

            // set ES_JAVA_OPTS environment variable if necessary
            val env = HashMap(System.getenv())
            if (!env.containsKey("ES_JAVA_OPTS")) {
                val javaOpts = config.getString(ConfigConstants.INDEX_ELASTICSEARCH_JAVA_OPTS)
                if (javaOpts != null) {
                    env["ES_JAVA_OPTS"] = javaOpts
                }
            }

            try {
                executor!!.execute(cmdl, env, object : DefaultExecuteResultHandler() {
                    override fun onProcessComplete(exitValue: Int) {
                        log.info("Elasticsearch quit with exit code: $exitValue")
                    }

                    override fun onProcessFailed(e: ExecuteException) {
                        if (!stopped) {
                            log.error("Elasticsearch execution failed", e)
                        }
                    }
                })
                f.complete()
            } catch (e: IOException) {
                f.fail(e)
            }
        }.toCompletable()
    }

    /**
     * Wait 60 seconds or until Elasticsearch is up and running, whatever
     * comes first
     * @param client the client to use to check if Elasticsearch is running
     * @return a Completable that will complete when Elasticsearch is running
     */
    fun waitUntilElasticsearchRunning(
            client: ElasticsearchClient): Completable {
        val repeat = NoStackTraceThrowable("")
        return Single.defer<Boolean>(Callable<Single<Boolean>> { client.isRunning }).flatMap { running ->
            if (!running) {
                return@Single.defer(client::isRunning).flatMap Single . error < Boolean >(repeat)
            }
            Single.just(running)
        }.retryWhen { errors ->
            val o = errors.flatMap { t ->
                if (t === repeat) {
                    // Elasticsearch is still not up, retry
                    return@errors.flatMap Observable . just < out Throwable > t
                }
                // forward error
                Observable.error<Throwable>(t)
            }
            // retry for 60 seconds
            RxUtils.makeRetry(60, 1000, null).call(o)
        }.toCompletable()
    }

    /**
     * Stop a running Elasticsearch instance
     */
    fun stop() {
        if (executor != null && !stopped) {
            stopped = true
            executor!!.watchdog.destroyProcess()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ElasticsearchRunner::class.java)
    }
}

package zookeeper

import org.apache.log4j.Logger

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


class PoolableJob extends Configurable {

    ExecutorService pool
    long poolTimeout = 1000 * 10l
    List<Worker> workers
    Closure process

    static final Logger log = Logger.getLogger(this)

    ExecutorService createPool(List threadConfig) {
        return Executors.newFixedThreadPool(threadConfig.size())
    }

    Worker getWorker(Integer id, Map data) {
        new Worker(id, this, data, process)
    }

    @Override
    void rebuild() {
        destroyArtifacts()
        pool = createPool(configuration.threads)
        workers = configuration.threads.collect { getWorker(it.id, it.data) }
        workers.each {pool.submit(it)}
    }


    @Override
    void destroyArtifacts() {
        workers*.stop()
        pool?.shutdown()
        if (!pool?.awaitTermination(poolTimeout, TimeUnit.MILLISECONDS))
            pool?.shutdownNow()
    }

    void notifyTermination(Integer id) {
        workers.remove(workers.find{it.id == id})
        Worker worker = getWorker(id, configuration.threads.find{it.id == id}.data)
        workers << worker
        pool.submit(worker)
    }
}

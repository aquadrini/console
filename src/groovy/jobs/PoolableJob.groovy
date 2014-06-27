package jobs

import jobs.Configurable
import jobs.builders.WorkerBuilder
import jobs.workers.Worker
import org.apache.log4j.Logger

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


class PoolableJob extends Configurable {

    ExecutorService pool
    long poolTimeout = 1000 * 10l
    List<Worker> workers
    Closure process
    WorkerBuilder builder

    static Logger log = Logger.getLogger(this)


    public PoolableJob(String name, WorkerBuilder builder, Closure process) {
        super(name)
        this.process = process
        this.builder = builder
    }

    ExecutorService createPool(List threadConfig) {
        return Executors.newFixedThreadPool(threadConfig.size())
    }

    Worker getWorker(Integer id, Map data) {
        builder.build(id, this, data, process)
    }

    @Override
    void rebuild() {
        destroyArtifacts()
        pool = createPool(getFromConfig('threads'))
        workers = Collections.synchronizedList(getFromConfig('threads').collect { getWorker(it.id, it.data) })
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
        try {
            if (lockOnConfig()) {
                workers.remove(workers.find { it.id == id })
                Worker worker = getWorker(id, getFromConfig('threads').find { it.id == id }.data)
                workers << worker
                pool.submit(worker)
            } else {
                log.error "Could not obtains lock for thread ${Thread.currentThread} restart."
            }
        } finally {
            unlockFromConfig()
        }
    }
}

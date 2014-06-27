package jobs

import grails.util.GrailsUtil
import jobs.builders.WorkerBuilder
import jobs.workers.Worker
import org.apache.log4j.Logger

import java.util.concurrent.CompletionService
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


class PoolableJob extends Configurable {

    CompletionService pool
    ExecutorService executors
    long poolTimeout = 1000 * 10l
    long waitTimeout = 1000 * 60l
    List<Worker> workers
    Closure process
    WorkerBuilder builder
    Boolean isStarted
    Object shutdownLock = new Object()

    static Logger log = Logger.getLogger(this)


    public PoolableJob(String name, WorkerBuilder builder, Closure process) {
        super(name)
        this.process = process
        this.builder = builder
        isStarted = false
    }

    ExecutorService createPool(List threadConfig) {
        return Executors.newFixedThreadPool(threadConfig.size())
    }

    Worker getWorker(Integer id, Map data) {
        builder.build(id, this, data, process)
    }

    @Override
    void rebuild() {
        int retries = 5
        while (retries--) {
            if (!pool || destroyArtifacts()) {
                executors = createPool(getFromConfig('threads'))
                pool = new ExecutorCompletionService(executors)
                workers = Collections.synchronizedList(getFromConfig('threads').collect { getWorker(it.id, it.data) })
                workers.each { pool.submit(it) }
                synchronized(shutdownLock) {
                    isStarted = true
                }
                return
            }
        }

        log.error "Could not stop previous instances for $name"
    }


    @Override
    Boolean destroyArtifacts() {
        try {
            if (lockOnConfig()) {
                workers*.stop()
                waitForThreads()
                executors?.shutdown()
                if (!executors?.awaitTermination(poolTimeout, TimeUnit.MILLISECONDS)) {
                    executors?.shutdownNow()
                    return executors?.awaitTermination(poolTimeout, TimeUnit.MILLISECONDS)
                }
                return true
            } else
                return false
        } catch (Exception e) {
                log.error "Exception waiting for threads in $name", GrailsUtil.sanitize(e)
                return false
        } finally {
            unlockFromConfig()
        }
    }

    void notifyTermination(Integer id) {
        synchronized(shutdownLock) {
            try {
                if (lockOnConfig()) {
                    workers.remove(workers.find { it.id == id })
                    Worker worker = getWorker(id, getFromConfig('threads').find { it.id == id }.data)
                    workers << worker
                    pool.submit(worker)
                } else {
                    log.error "Could not obtains lock for thread ${Thread.currentThread().id} restart."
                }
            } finally {
                unlockFromConfig()
            }
        }
    }

    Boolean canShutdown() {
        synchronized (shutdownLock) {
            return isStarted
        }
    }

    void waitForThreads() {
        workers.size().times {
            Integer id = pool.poll()?.get(waitTimeout, TimeUnit.MILLISECONDS)
            log.info "Thread $id finished in $name"
        }
    }

}

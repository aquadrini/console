package jobs.workers

import jobs.PoolableJob

import java.util.concurrent.atomic.AtomicBoolean

class ContinuousWorker extends OneShotWorker {

    AtomicBoolean run

    public ContinuousWorker(Integer id, PoolableJob parent, Map config, Closure process) {
        super(id, parent, config, process)
        run = new AtomicBoolean(true)
    }

    Integer call() {
        log.info "Initializing worker ${id}."
        while (run.get() && !Thread.interrupted()) {
            doWork()
        }
        log.info "Worker $id finished."
        return id
    }

    void stop() {
        run.set(false)
    }

}

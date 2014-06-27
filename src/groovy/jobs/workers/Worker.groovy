package jobs.workers

import jobs.PoolableJob
import org.apache.log4j.Logger

import java.util.concurrent.Callable


abstract class Worker implements Callable<Integer> {

    Integer id
    PoolableJob parent
    Map config
    Closure process

    Logger log = Logger.getLogger(this.class)

    abstract void doWork()
    abstract void stop()

    public Worker(Integer id, PoolableJob parent, Map config, Closure process) {
        this.id = id
        this.parent = parent
        this.config = config
        this.process = process
    }

    Integer call() {
        log.info "Initializing worker ${id}."
        doWork()
        log.info "Worker $id finished."
        return id
    }

}

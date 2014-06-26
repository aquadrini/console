package zookeeper

import org.apache.log4j.Logger

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicBoolean


class Worker implements Callable<Integer> {

    Integer id
    PoolableJob parent
    Map config
    Closure process

    Logger log = Logger.getLogger(this)

    public Worker(Integer id, PoolableJob parent, Map config, Closure process) {
        this.id = id
        this.parent = parent
        this.config = config
        this.process = process
    }

    void stop() { }

    void doWork() {
        try {
            process(id, config)
        } catch (Exception e) {
            log.error "Exception during message processing.", GrailsUtil.deepSanitize(e)
            parent.notifyTermination(id)
        }
    }

    Integer call() {
        log.info "Initializing worker ${id}."
        doWork()
        log.info "Worker $id finished."
        return id
    }

}

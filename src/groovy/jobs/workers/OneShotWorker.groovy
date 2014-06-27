package jobs.workers

import jobs.PoolableJob

class OneShotWorker extends Worker {

    OneShotWorker(Integer id, PoolableJob parent, Map config, Closure process) {
        super(id, parent, config, process)
    }

    void doWork() {
        try {
            process(id, config)
        } catch (Exception e) {
            log.error "Exception during message processing.", GrailsUtil.deepSanitize(e)
            parent.notifyTermination(id)
        }
    }

    void stop() {}

}

package jobs.builders

import jobs.Configurable
import jobs.workers.OneShotWorker
import jobs.workers.Worker

class OneShotWorkerBuilder implements WorkerBuilder {

    @Override
    Worker build(Integer id, Configurable monitor, Map data, Closure process) {
        return new OneShotWorker(id, monitor, data, process)
    }

}

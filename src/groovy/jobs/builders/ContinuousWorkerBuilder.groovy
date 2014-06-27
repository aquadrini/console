package jobs.builders

import jobs.Configurable
import jobs.workers.ContinuousWorker
import jobs.workers.Worker

class ContinuousWorkerBuilder implements WorkerBuilder {

    @Override
    Worker build(Integer id, Configurable monitor, Map data, Closure process) {
        return new ContinuousWorker(id, monitor, data, process)
    }
}

package jobs.builders

import jobs.Configurable
import jobs.workers.Worker


interface WorkerBuilder {

    Worker build(Integer id, Configurable monitor, Map data, Closure process)

}

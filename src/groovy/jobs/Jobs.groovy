package jobs

import jobs.builders.ContinuousWorkerBuilder
import jobs.builders.OneShotWorkerBuilder


class Jobs {

    static PoolableJob poolableOneShot(String name, Closure process ) {
        return new PoolableJob(name, new OneShotWorkerBuilder(), process)
    }

    static PoolableJob poolableContinuous(String name, Closure process ) {
        return new PoolableJob(name, new ContinuousWorkerBuilder(), process)
    }

}

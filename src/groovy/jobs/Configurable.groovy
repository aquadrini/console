package jobs

import grails.converters.JSON
import jobs.zookeeper.ClientMonitor
import org.apache.log4j.Logger
import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock


abstract class Configurable implements DataListener<String> {

    static final long READ_TIMEOUT = 30 * 1000l
    static final long WRITE_TIMEOUT = 60 * 1000l

    static Logger log = Logger.getLogger(this)

    private ClientMonitor monitor
    private ReentrantReadWriteLock lock
    private Map configuration
    String name

    public Configurable(String name) {
        this.name = name
        String path = CH.config.zookeeper.jobPrefix + name + '/' + host
        lock = new ReentrantReadWriteLock(true)
        monitor = new ClientMonitor(
                CH.config.zookeeper.hosts.collect{it.host+':'+it.port}.join(','),
                CH.config.zookeeper.defaultTimeout,
                path,
                this
        )
    }

    abstract void rebuild()
    abstract Boolean destroyArtifacts()
    abstract Boolean canShutdown()

    String getHost() {
        Process proc = 'hostname'.execute()
        proc.waitFor()
        return proc.text.replaceAll(/\s/,'')
    }

    void init() {
        monitor.watch()
    }

    Boolean destroy() {
        if (canShutdown() && lockOnConfig()) {
            monitor.shutdown()
            return destroyArtifacts()
        } else
            return false
    }

    def getFromConfig(String key) {
        def output
        if (lockOnConfig()) {
            output = configuration[key]
            unlockFromConfig()
        } else {
            log.error "Waited for too much to read config for ${key}, exiting with error..."
        }

        return output
    }

    boolean lockOnConfig() {
        return lock.readLock().tryLock(READ_TIMEOUT, TimeUnit.MILLISECONDS)
    }

    void unlockFromConfig() {
        lock.readLock().unlock()
    }


    void process(String data) {
        boolean s = lock.writeLock().tryLock(WRITE_TIMEOUT, TimeUnit.MILLISECONDS)
        try {
            if (s) {
                configuration = JSON.parse(data) as Map
                rebuild()
            } else {
                log.error "Waited for too much to write config, exiting with error..."
            }
        } finally {
            lock.writeLock().unlock()
        }
    }



}

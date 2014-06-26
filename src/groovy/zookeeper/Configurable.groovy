package zookeeper

import grails.converters.JSON

import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH


abstract class Configurable implements DataListener<String> {

    ClientMonitor monitor
    Map configuration

    public Configurable() {
        String path = CH.config.zookeeper.jobprefix + this.class.simpleName  + '/' + host
        monitor = new ClientMonitor(
                CH.config.zookeeper.hosts.collect{it.host+':'+it.port}.join(','),
                CH.config.zookeeper.defaultTimeout,
                path,
                this
        )
    }

    abstract void rebuild()
    abstract void destroyArtifacts()

    String getHost() {
        Process proc = 'hostname'.execute()
        proc.waitFor()
        return proc.text.replaceAll(/\s/,'')
    }

    void init() {
        monitor.watch()
    }

    void destroy() {
        monitor.shutdown()
        destroyArtifacts()
    }

    void process(String data) {
        configuration = JSON.parse(data) as Map
        rebuild()
    }



}

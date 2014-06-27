package jobs.zookeeper

import grails.util.GrailsUtil
import jobs.DataListener
import org.apache.log4j.Logger
import org.apache.zookeeper.AsyncCallback.StatCallback
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import static org.apache.zookeeper.Watcher.Event.EventType.*
import static org.apache.zookeeper.Watcher.Event.KeeperState.*
import static org.apache.zookeeper.KeeperException.Code.*

class ClientMonitor implements Watcher, StatCallback {

    static final Logger log = Logger.getLogger(this)

    ZooKeeper zk
    Watcher next
    String node
    String hosts
    int timeout
    byte[] data
    DataListener listener

    public ClientMonitor(String hosts, int timeout, String node, DataListener listener, Watcher next=null) {
        this.hosts = hosts
        this.timeout = timeout
        this.node = node
        this.next = next
        this.listener = listener

        connect()
    }

    void connect() {
        try {
            zk?.close()
            zk = new ZooKeeper(hosts, timeout, this)
        } catch (IOException e) {
            log.error "Could not create zk client on $hosts", GrailsUtil.sanitize(e)
        }
    }

    void watch() {
        log.info "Connecting to ZK"
        zk?.exists(node, true, this, null)
    }

    @Override
    void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists
        switch (KeeperException.Code.get(rc)) {
            case OK:
                exists = true
                break;
            case NONODE:
                log.info "No node ${path}. Someone removed it?"
                exists = false
                break
            case SESSIONEXPIRED:
            case CONNECTIONLOSS:
                connect()
                watch()
                break
            case NOAUTH:
                log.error "Authentication error for $node in $hosts"
                return
            default:
                log.error "Unexpected return code ${rc} (${KeeperException.Code.get(rc)}) for $node"
                watch()
                return
        }

        byte[] b

        if (exists) {
            try {
                b = zk.getData(node, false, null);
            } catch (KeeperException e) {
                log.error "Exception reading data from $node", GrailsUtil.sanitize(e)
            } catch (InterruptedException e) {
                return
            }
        }

        if (b && !Arrays.equals(data, b))
            listener.process(new String(b))

    }

    @Override
    void process(WatchedEvent event) {
        String path = event.path
        if (event.type == None) {
            switch (event.state) {
                case SyncConnected:
                    break
                case Expired:
                    log.warn "Expired session for ${this.class.name}: ${Thread.currentThread().id}"
                    connect()
                    watch()
                    break
                case Disconnected:
                    log.warn "Disconnected from ZK in ${this.class.name}: ${Thread.currentThread().id}"
                    connect()
                    watch()
                    break
                default:
                    log.error "Unknown event state ${event.state}"
            }
        } else {
            if (path?.equals(node))
                zk.exists(node, true, this, null)
        }

        next?.process(event)
    }

    void shutdown() {
        zk?.close()
    }

}

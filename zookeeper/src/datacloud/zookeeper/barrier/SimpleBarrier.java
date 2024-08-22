package datacloud.zookeeper.barrier;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import static datacloud.zookeeper.util.ConfConst.*;

import datacloud.zookeeper.ZkClient;

public class SimpleBarrier implements Watcher{

    private ZkClient client;
    private String path;


    public SimpleBarrier(ZkClient client, String path) throws KeeperException, InterruptedException {
        this.client = client;
        this.path = path;

        // Si le znode n'existe pas alors on l'initialise 
        if(client.zk().exists(path, false) == null) {
            client.zk().create(path, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }


    public synchronized void sync() {
        // l'appelant de se mettre en attente tant que le znode caractérisant la barrière existe.
        try {
            while(client.zk().exists(path, this) != null){
                this.wait();
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(WatchedEvent event) {
        if(event.getType() == EventType.NodeDeleted)
            this.notifyAll();        
    }
    
}

package datacloud.zookeeper.barrier;

import java.math.BigInteger;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;
import datacloud.zookeeper.util.ConfConst;

public class BoundedBarrier implements Watcher {
    private ZkClient client;
    private String path;
    private int N;
    private boolean flag = false;

    public BoundedBarrier(ZkClient client, String path, int N) throws KeeperException, InterruptedException {
        this.client = client;
        this.path = path;

        // si le znode n'existe pas, il est alors créé en preannt en compte l'entier
        if(client.zk().exists(path, false) == null) {
          this.N = N;
          BigInteger bigInt = BigInteger.valueOf(N);
          client.zk().create(path, bigInt.toByteArray() , Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
          this.N = new BigInteger(client.zk().getData(path, this, null) ).intValue();
        }

    }


    /**
     *  offrir une méthode sync() qui est bloquant tant que N clients existants n’ont pas fait appel à
     *  sync. Il faudra assurer que tout znode ayant servi au bon fonctionnement de cette 
     * synchronisation soit supprimé une fois que tous les zkclients ont été réveillés 
     * et que la barrière n’est plus utilisée.
     */

    public synchronized void sync() {

      try {
        
        client.zk().create(path+"/", ConfConst.EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        
        if(flag != false && sizeBarrier() == 0) {
          flag = true;
          List<String> children = client.zk().getChildren(path, false); 
          
          for(String c : children) {
            client.zk().delete(path+"/"+c, 0);
          }

          //suppression de la barrière
          client.zk().delete(path, 0);
        } else {
          // tant que N clients n'ont pas fait appel à la méthode sync
          while(sizeBarrier() > 0) {
            this.wait();
          }
        }
      } catch (InterruptedException | KeeperException e) {
        e.printStackTrace();
      }

    }

    public synchronized int sizeBarrier() throws KeeperException, InterruptedException {
        return N - client.zk().getChildren(path, this).size();
    }

    @Override
    public synchronized void process(WatchedEvent event) {
      if(event.getType() == EventType.NodeChildrenChanged)
        this.notifyAll();
    }
    
}

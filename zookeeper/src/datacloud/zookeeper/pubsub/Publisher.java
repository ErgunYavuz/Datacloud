package datacloud.zookeeper.pubsub;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class Publisher extends ZkClient {

  public Publisher(String name, String servers) throws IOException, KeeperException, InterruptedException {
      super(name, servers);
  }

  public void publish(String topic, String message) throws KeeperException, InterruptedException {
    // si 
    if(zk().exists(topic, null) == null)
      zk().create(topic+"/", message.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    else
      zk().setData(topic+"/", message.getBytes(), zk().exists("/"+topic, false).getAversion());
  }

  @Override
  public void process(WatchedEvent arg0) {        
  }
    
}

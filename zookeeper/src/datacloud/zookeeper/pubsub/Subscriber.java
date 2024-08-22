package datacloud.zookeeper.pubsub;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import datacloud.zookeeper.ZkClient;

public class Subscriber extends ZkClient {

  private Map<String, List<String>> messages = new HashMap<String, List<String>>();

  public Subscriber(String name, String servers) throws IOException, KeeperException, InterruptedException {
    super(name, servers);
  }

  public void subscribe(String topic) {

  }

  public List<String> received(String topic) {
    List<String> msgs = messages.get(topic);
    if(msgs != null) 
      return msgs;
    return Collections.emptyList();
  }

  @Override
  public void process(WatchedEvent arg0) {
    // TODO Auto-generated method stub
      
  }
    
}

package datacloud.zookeeper.membership;

import static datacloud.zookeeper.util.ConfConst.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import datacloud.zookeeper.ZkClient;

public class ClientMembership extends ZkClient{
    private List<String> members;

    public ClientMembership(String name, String servers) throws IOException, KeeperException, InterruptedException {
        super(name, servers);
        
        members = new ArrayList<>();

    }

    /**
     * Retourne la liste locale des clients connectés du point de vue de l'instance
     * @return une liste des clients
     */
    public List<String> getMembers() {
        return members;
    }

    /**
     * Ainsi à chaque départ d’un zkclient ou à chaque arrivée d’un nouveau zkclient, 
     * la liste locale de chaque zkclient connecté doit être mise à jour.
     */

    @Override
    public void process(WatchedEvent event) {
        if(event.getType() == EventType.NodeChildrenChanged){
            try {
                members = this.zk().getChildren(ZNODEIDS, true);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        } 
        
    }
    
}

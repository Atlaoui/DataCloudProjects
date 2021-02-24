package datacloud.zookeeper.membership;

import static datacloud.zookeeper.util.ConfConst.ZNODEIDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datacloud.zookeeper.ZkClient;

public class ClientMembership extends ZkClient {
	private List<String> members;
	private static int cpt =0;
	private final String myName ;
	public ClientMembership(String name, String servers) throws IOException, KeeperException, InterruptedException {
		super(name, servers);
		members = Collections.emptyList();
		myName ="C"+cpt++;
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case SyncConnected:
				updateMyList();
				break;
			case Closed:
				updateMyList();
				break;
			case Disconnected:
				updateMyList();
				break;
			default:
				break;

			}
		}else if(event.getType() == Event.EventType.NodeCreated || event.getType() == Event.EventType.NodeChildrenChanged
				|| event.getType() == Event.EventType.NodeDeleted ){
			updateMyList();
		}

	}

	private synchronized void updateMyList() {
		try {if(zk().exists(ZNODEIDS, true) !=null) 
			members =super.zk().getChildren(ZNODEIDS, true);} 
		catch (KeeperException | InterruptedException e) {e.printStackTrace();}

	}

	public synchronized List<String> getMembers() {
		//sol 1
		//updateMyList();
		return members;
	}

	public String getMyName() {
		return myName;
	}
}

package datacloud.zookeeper.pubsub;


import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import datacloud.zookeeper.ZkClient;

public class Publisher extends ZkClient{
	private Stat version = new Stat();
	private static int cpt =0;
	private final int id;

	public Publisher(String name, String servers)  throws IOException, KeeperException, InterruptedException  {
		super(name, servers);
		id=cpt++; 
	}

	public void publish(String topic, String message) {
		try {		
			if(zk().exists("/"+topic, this) == null) {
				zk().create("/"+topic, message.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				version= zk().setData("/"+topic, message.getBytes(), 0);
			}
			else {
				byte[] datachange = null;
				do {	
					datachange  = zk().getData("/"+topic, true, version);
					if(datachange != null)
						zk().setData("/"+topic, message.getBytes(), -1);
				}while(datachange == null);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		//osef
	}

}

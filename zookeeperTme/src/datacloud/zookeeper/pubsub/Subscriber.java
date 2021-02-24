package datacloud.zookeeper.pubsub;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import datacloud.zookeeper.ZkClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
public class Subscriber extends ZkClient{
	private ConcurrentHashMap<String,List<String>> mapTopic = new ConcurrentHashMap<>();
	
	private Stat myVersion = new Stat();
	private static int cpt =0;
	private final int myId; 
	public Subscriber(String name, String servers)  throws IOException, KeeperException, InterruptedException  {
		super(name, servers);
		myId=cpt++; 
	}

	public synchronized List<String> received(String topic) {
		if(mapTopic.containsKey(topic))
			return mapTopic.get(topic);
		return Collections.emptyList();
	}

	public synchronized void subscribe(String topic) {
		try {
			zk().exists("/"+topic, this);
			List<String> l = Collections.synchronizedList(new ArrayList<>());
			mapTopic.put(topic, l);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		if(event.getType() == Event.EventType.DataWatchRemoved ) {
			try { 
			zk().exists(event.getPath(),this);
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}else if(event.getType() == Event.EventType.NodeCreated ) {
			try {
				String[] tmp  = event.getPath().split("/");
				String topic = tmp[tmp.length-1];
				if(mapTopic.containsKey(topic))
				  zk().exists(event.getPath(),this);
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		else if(event.getType() == Event.EventType.NodeDataChanged ) {
			byte[] datachange = null;
			do {
				try {
					String[] tmp  = event.getPath().split("/");
					String topic = tmp[tmp.length-1];

					datachange = zk().getData(event.getPath(),this, myVersion);
					if(mapTopic.containsKey(topic)) {
						List<String> l =Collections.synchronizedList(mapTopic.get(topic));
						l.add(new String(datachange,StandardCharsets.UTF_8));
						mapTopic.put(topic, l);
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}while(datachange == null);
		}

	}
}



package datacloud.zookeeper.barrier;


import static datacloud.zookeeper.util.ConfConst.EMPTY_CONTENT;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import datacloud.zookeeper.ZkClient;

public class SimpleBarrier implements Watcher {

	private String znodeBarrier;
	private ZkClient myClient;
	private Object mutex = new Object();
	public SimpleBarrier(ZkClient clientRunnable, String znodeBarrier) {
		this.znodeBarrier=znodeBarrier;
		this.myClient=clientRunnable;
		try {
			if(myClient.zk().exists(znodeBarrier, this) == null) {
				myClient.zk().create(znodeBarrier, EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized void sync() {
		try {
			if(myClient.zk().exists(znodeBarrier, this)!=null) {
				this.wait();
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		this.notify();
	}

}

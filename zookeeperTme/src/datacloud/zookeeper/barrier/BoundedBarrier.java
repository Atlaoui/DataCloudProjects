package datacloud.zookeeper.barrier;



import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import datacloud.zookeeper.ZkClient;
import datacloud.zookeeper.util.ConfConst;

public class BoundedBarrier implements Watcher{
	private static int cpt =0;
	private final int id; 
	private String znodeBarrier;
	private String syncZnode ;
	private ZkClient myClient;
	private String name = "Node";
	private int size;
	private static final Object mutex = new Object();
	private  boolean isDestroyer; 


	public BoundedBarrier(ZkClient clientRunnable, String znodeBarrier,Integer size) {

		this.znodeBarrier=znodeBarrier;
		String[] tmp  = znodeBarrier.split("/");
		String nomDeBarrier = tmp[tmp.length-1];
		syncZnode = "/Sync"+nomDeBarrier;

		this.myClient=clientRunnable;
		id=cpt++;
		synchronized (mutex) {
			try {
				if(myClient.zk().exists(znodeBarrier, false) == null) {
					myClient.zk().create(znodeBarrier,BigInteger.valueOf(size).toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					myClient.zk().create(syncZnode,ConfConst.EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					this.size = size;
					System.out.println(id+": crée le node");
					isDestroyer=true;

				}
				else {
					this.size = getMyData();
					isDestroyer=false;

					//System.out.println("les enfant : "+myClient.zk().getChildren(znodeBarrier, false));
					System.out.println(id+": ne crée pas le node");
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}


	}

	public void sync() {
		try {
			myClient.zk().create(syncZnode + "/" + name+id, ConfConst.EMPTY_CONTENT, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
			while (myClient.zk().exists(znodeBarrier, true) !=null ) {
				synchronized (mutex) {	
					List<String> list = myClient.zk().getChildren(syncZnode, this);
					if (list.size() < size) {
						mutex.wait();
					}
					else if(myClient.zk().exists(znodeBarrier, true) !=null ){
						myClient.zk().delete(znodeBarrier, 0);
					}
				}
			}

			//on vire tous les znode
			myClient.zk().delete(syncZnode + "/" + name+id, 0);

			if(myClient.zk().exists(syncZnode, false) !=null ){
				myClient.zk().delete(znodeBarrier, 0);
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}



	public int sizeBarrier() {
		return size;
	}

	@Override
	public  void process(WatchedEvent event) {
		//System.out.println(id+": a recus  "+event);
		if(event.getType() == Event.EventType.None ) {
			//dans le cas ou un process a était fermer
			if(event.getState() == KeeperState.Closed) {
				try {
					List<String> list = myClient.zk().getChildren(syncZnode, this);
					if(myClient.zk().exists(znodeBarrier, true) !=null ){
						myClient.zk().delete(znodeBarrier, 0);
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
		synchronized (mutex) {
			mutex.notifyAll();
		}

	}

	private int getMyData() throws KeeperException, InterruptedException {
		byte[] ret = myClient.zk().getData(znodeBarrier, false, null);
		return  new BigInteger(ret).intValue();
	}
}



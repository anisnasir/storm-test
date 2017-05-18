package contributions;

import java.util.HashSet;

public class VirtualWorker {
	long load;
	int worker;
	public VirtualWorker(long load, int worker) { 
		this.load = load;
		this.worker= worker;
	}
	public long getLoad() {
		return load;
	}
	public void setLoad(long load) {
		this.load = load;
	}
	public int getWorker() {
		return worker;
	}
	public void setWorker(int worker) {
		this.worker = worker;
	}
	public void incrementNumberMessage() {
		load++;
	}
}

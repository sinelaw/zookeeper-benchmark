package edu.brown.cs.zkbenchmark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.log4j.Logger;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

	AtomicInteger _totalOps;
	private boolean _syncfin;

	private static final Logger LOG = Logger.getLogger(SyncBenchmarkClient.class);

	
	public SyncBenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
			int attempts, int id) throws IOException {
		super(zkBenchmark, host, namespace, attempts, id);
	}
	
	@Override
	protected void submit(int n, TestType type) {
		try {
			submitWrapped(n, type);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception
			LOG.error("Error while submitting requests", e);
		}
	}

    class OpTime {
        public long startTime;
        public long endTime;
        public OpTime(long s, long e) {
            startTime = s;
            endTime = e;
        }
    };

	protected void submitWrapped(int n, TestType type) throws Exception {
		_syncfin = false;
		_totalOps = _zkBenchmark.getCurrentTotalOps();
		byte data[];

                long testStart = System.nanoTime();
                OpTime[] latencies = new OpTime[1000 * 1000];
		for (int i = 0; i < _totalOps.get(); i++) {
			long submitTime = System.nanoTime();

			switch (type) {
				case READ:
					_client.getData().forPath(_path);
					break;

				case SETSINGLE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.setData().forPath(_path, data);
					break;

				case SETMULTI:
					try {
						data = new String(_zkBenchmark.getData() + i).getBytes();
						_client.setData().forPath(_path + "/" + (_count % _highestN), data);
					} catch (NoNodeException e) {
						LOG.warn("No such node when setting data to mutiple nodes. " +
					             "_path = " + _path + ", _count = " + _count +
					             ", _highestN = " + _highestN, e);
					}
					break;

				case CREATE:
					data = new String(_zkBenchmark.getData() + i).getBytes();
					_client.create().forPath(_path + "/" + _count, data);
					_highestN++;
					break;

				case DELETE:
					try {
						_client.delete().forPath(_path + "/" + _count);
					} catch (NoNodeException e) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("No such node (" + _path + "/" + _count +
									") when deleting nodes", e);
						}
					}
			}

                        long endTime = System.nanoTime();
                        latencies[i] = new OpTime(submitTime, endTime);
			_count++;
			_zkBenchmark.incrementFinished();

			if (_syncfin)
				break;
		}
                long duration = System.nanoTime() - testStart;
                int durationSecs = (int)(duration / (1000 * 1000 * 1000)) + 1;
                long[] avgLatencies = new long[durationSecs];
                long[] binSizes = new long[durationSecs];
                for (int i = 0; i < durationSecs; i++) {
                    avgLatencies[i] = 0;
                    binSizes[i] = 0;
                }
                for (int i = 0; i < _count; i++) {
                    int sec = (int)((latencies[i].startTime - testStart) / (1000 * 1000 * 1000));
                    avgLatencies[sec] += (latencies[i].endTime - latencies[i].startTime) / 1000; // micros
                    binSizes[sec]++;
                }
                for (int i = 0; i < avgLatencies.length; i++) {
                    long res = binSizes[i] == 0 ? 0 : avgLatencies[i]/binSizes[i];
                    try {
                        _latenciesFile.write(Integer.toString(i) + " " + res + "\n");
                    } catch (IOException e) {
                        LOG.error("Exceptions while writing to file", e);
                    }

                }

	}
	
	@Override
	protected void finish() {
		_syncfin = true;
	}

	/**
	 * in fact, n here can be arbitrary number as synchronous operations can be stopped 
	 * after finishing any operation.
	 */
	@Override
	protected void resubmit(int n) {
		_totalOps.getAndAdd(n);
	}
}

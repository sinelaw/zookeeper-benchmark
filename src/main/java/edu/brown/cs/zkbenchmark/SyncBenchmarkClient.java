package edu.brown.cs.zkbenchmark;

import java.util.Random;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.log4j.Logger;

import edu.brown.cs.zkbenchmark.ZooKeeperBenchmark.TestType;

public class SyncBenchmarkClient extends BenchmarkClient {

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
            throw new RuntimeException(e);
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
        byte data[];

        LOG.debug("Starting job");
        long testStart = System.nanoTime();
        Random random = new Random();
        OpTime[] latencies = new OpTime[1000 * 1000];
        int ops = _zkBenchmark.getTotalOps() / _zkBenchmark.getClients();
        for (int i = 0; i < ops; i++) {
            long submitTime = System.nanoTime();

            switch (type) {
            case READ:
                _client.getData().forPath(_path);
                break;

            case SETSINGLE:
                data = new String(_zkBenchmark.getData() + i).getBytes();
                _client.setData().forPath("/singleKey", data);
                break;

            case SETMULTI:
                long key = random.nextInt() % _zkBenchmark.getKeys();
                data = new String(_zkBenchmark.getData() + key).getBytes();
                _client.setData().forPath("/" + key, data);
                break;

            case CREATE:
                if (i < _zkBenchmark.getKeys()) {
                    try {
                        _client.delete().forPath("/" + i);
                    } catch (NoNodeException e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("No such node (" + "/" + i +
                                      ") when deleting nodes", e);
                        }
                    }
                    data = new String(_zkBenchmark.getData() + i).getBytes();
                    _client.create().forPath("/" + i, data);
                    _highestN++;
                }
                break;

            case DELETE:
                try {
                    _client.delete().forPath("/" + i);
                } catch (NoNodeException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No such node (" + "/" + i +
                                  ") when deleting nodes", e);
                    }
                }
            }

            long endTime = System.nanoTime();
            latencies[i] = new OpTime(submitTime, endTime);
            _count++;

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
}

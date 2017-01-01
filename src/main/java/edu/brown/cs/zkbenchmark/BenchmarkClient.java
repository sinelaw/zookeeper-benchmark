package edu.brown.cs.zkbenchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.Random;

import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.retry.RetryNTimes;

public class BenchmarkClient implements Callable<RunResult> {
    protected ZooKeeperBenchmark _zkBenchmark;
    protected String _host; // the host this client is connecting to
    protected CuratorFramework _client; // the actual client
    protected int _attempts;
    protected String _path;
    protected int _id;
    protected int _count;
    protected int _countTime;
    protected Timer _timer;

    protected int _highestN;
    protected int _highestDeleted;

    protected BufferedWriter _latenciesFile;

    private static final Logger LOG = Logger.getLogger(BenchmarkClient.class);


    public BenchmarkClient(ZooKeeperBenchmark zkBenchmark, String host, String namespace,
                           int attempts, int id) throws IOException {
        _zkBenchmark = zkBenchmark;
        _host = host;
        _client = CuratorFrameworkFactory.builder()
            .connectString(_host).namespace(namespace)
            .retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
            .connectionTimeoutMs(5000).build();
        _attempts = attempts;
        _id = id;
        _path = "/client"+id;
        _timer = new Timer();
        _highestN = 0;
        _highestDeleted = 0;

        if (!_client.isStarted()) {
            _client.start();
        }
    }

    @Override
    public RunResult call() {
        zkAdminCommand("srst"); // Reset ZK server's statistics

        // Wait for all clients to be ready

        try {
            _zkBenchmark.getBarrier().await();
        } catch (InterruptedException e) {
            LOG.warn("Client #" + _id + " was interrupted while waiting on barrier", e);
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            LOG.warn("Some other client was interrupted. Client #" + _id + " is out of sync", e);
            throw new RuntimeException(e);
        }

        _count = 0;
        _countTime = 0;

        // Create a directory to work in

        try {
            Stat stat = _client.checkExists().forPath(_path);
            if (stat == null) {
                _client.create().forPath(_path, _zkBenchmark.getData().getBytes());
            }
        } catch (Exception e) {
            LOG.error("Error while creating working directory", e);
            throw new RuntimeException(e);
        }

        // Create a timer to check when we're finished. Schedule it to run
        // periodically in case we want to record periodic statistics
        // Submit the requests!

        return submit(_attempts);


        // try {
        //     _latenciesFile = new BufferedWriter(new FileWriter(new File(_id +
        //                                                                 "-" + _type + "_timings.dat")));
        // } catch (IOException e) {
        //     LOG.error("Error while creating output file", e);
        //     throw new RuntimeException(e);
        // }


        // Test is complete. Print some stats and go home.

        // zkAdminCommand("stat");


        // try {
        //     if (_latenciesFile != null)
        //         _latenciesFile.close();
        // } catch (IOException e) {
        //     LOG.warn("Error while closing output file:", e);
        //     throw new RuntimeException(e);
        // }

        // LOG.info("Client #" + _id + " -- Current test complete. " +
        //          "Completed " + _count + " operations.");

    }

    public void doCleaning() {
        try {
            deleteChildren();
        } catch (Exception e) {
            LOG.error("Exception while deleting old znodes", e);
        }

    }

    /* Delete all the child znodes created by this client */
    void deleteChildren() throws Exception {
        List<String> children;

        do {
            children = _client.getChildren().forPath(_path);
            for (String child : children) {
                _client.delete().inBackground().forPath(_path + "/" + child);
            }
            Thread.sleep(2000);
        } while (children.size() != 0);
    }


    void recordEvent(CuratorEvent event) {
        Double submitTime = (Double) event.getContext();
        double relEndTime = ((double)System.nanoTime() - _zkBenchmark.getStartTime())/1000000000.0;
        recordElapsedInterval(submitTime, relEndTime);
    }

    void recordElapsedInterval(Double start, Double end) {
        try {
            _latenciesFile.write(start.toString() + " " + Double.toString(end-start) + "\n");
        } catch (IOException e) {
            LOG.error("Exceptions while writing to file", e);
        }
    }

    /* Send a command directly to the ZooKeeper server */
    void zkAdminCommand(String cmd) {
        String host = _host.split(":")[0];
        int port = Integer.parseInt(_host.split(":")[1]);
        Socket socket = null;
        OutputStream os = null;
        InputStream is = null;
        byte[] b = new byte[1000];

        try {
            socket = new Socket(host, port);
            os = socket.getOutputStream();
            is = socket.getInputStream();

            os.write(cmd.getBytes());
            os.flush();

            int len = is.read(b);
            while (len >= 0) {
                LOG.info("Client #" + _id + " is sending " + cmd +
                         " command:\n" + new String(b, 0, len));
                len = is.read(b);
            }

            is.close();
            os.close();
            socket.close();
        } catch (UnknownHostException e) {
            LOG.error("Unknown ZooKeeper server: " + _host, e);
        } catch (IOException e) {
            LOG.error("IOException while contacting ZooKeeper server: " + _host, e);
        }
    }

    int getOpsCount(){
        return _count;
    }

    ZooKeeperBenchmark getBenchmark() {
        return _zkBenchmark;
    }

    protected RunResult submit(int n) {
        try {
            return submitWrapped(n);
        } catch (Exception e) {
            // What can you do? for some reason
            // com.netflix.curator.framework.api.Pathable.forPath() throws Exception
            LOG.error("Error while submitting requests", e);
            throw new RuntimeException(e);
        }
    }

    protected void doCreate() {
        try {
            for (int i = 0; i < _zkBenchmark.getKeys(); i++) {
                byte data[] = new String(_zkBenchmark.getData() + i).getBytes();
                try {
                    _client.delete().forPath("/" + i);
                } catch (NoNodeException e) {
                    // ignore
                }
                _client.create().forPath("/" + i, data);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected RunResult submitWrapped(int n) throws Exception {
        byte data[];

        RunResult result = new RunResult();

        LOG.debug("Starting job");
        Random random = new Random();
        int ops_per_client = _zkBenchmark.getTotalOps() / _zkBenchmark.getClients();
        int ops = ops_per_client;
        result.latencies = new RunResult.OpTime[ops];
        result.startNanos = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            long submitTime = System.nanoTime();

            long key = random.nextInt(_zkBenchmark.getKeys());
            data = new String(_zkBenchmark.getData() + key).getBytes();
            _client.setData().forPath("/" + key, data);

            long endTime = System.nanoTime();
            result.latencies[i] = new RunResult.OpTime(submitTime, endTime);
            _count++;

        }
        result.endNanos = System.nanoTime();
        result.numOps = ops;
        return result;
    }
}

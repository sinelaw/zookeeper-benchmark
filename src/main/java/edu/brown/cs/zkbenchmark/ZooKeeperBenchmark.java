package edu.brown.cs.zkbenchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

public class ZooKeeperBenchmark {
    private int _totalOps; // total operations requested by user
    private int _lowerbound;
    private BenchmarkClient[] _clients;
    private int _interval;
    private long _startCpuTime;
    private HashMap<Integer, FutureTask<RunResult>> _running;
    private String _data;
    private CyclicBarrier _barrier;
    private int _keys;

    private static final Logger LOG = Logger.getLogger(ZooKeeperBenchmark.class);

    class DaemonThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    }

    public ZooKeeperBenchmark(Configuration conf) throws IOException {
        LinkedList<String> serverList = new LinkedList<String>();
        Iterator<String> serverNames = conf.getKeys("server");

        while (serverNames.hasNext()) {
            String serverName = serverNames.next();
            String address = conf.getString(serverName);
            serverList.add(address);
        }

        if (serverList.size() == 0) {
            throw new IllegalArgumentException("ZooKeeper server addresses required");
        }

        _keys = conf.getInt("keys");
        _interval = conf.getInt("interval");
        _totalOps = conf.getInt("totalOperations");
        _lowerbound = conf.getInt("lowerbound");

        _running = new HashMap<Integer,FutureTask<RunResult>>();
        _clients = new BenchmarkClient[conf.getInt("clients")];
        _barrier = new CyclicBarrier(_clients.length+1);

        LOG.info("benchmark set with: interval: " + _interval + " total number: " + _totalOps +
                 " threshold: " + _lowerbound);

        _data = "";

        for (int i = 0; i < 8; i++) { // 8 bytes of important data
            _data += "!";
        }

        int avgOps = _totalOps / serverList.size();

        for (int i = 0; i < _clients.length; i++) {
            int server_idx = i % serverList.size();
            _clients[i] = new BenchmarkClient(this, serverList.get(server_idx), "/zkTest", i);
        }

    }

    public void runBenchmark() {

        doTest();

        // for (int i = 0; i < _clients.length; i++) {
        //     _clients[i].doCleaning();
        // }

        LOG.info("All tests are complete");
    }

    /* This is where each individual test starts */

    public void doTest() {
        _clients[0].doCreate();

        System.out.print("Runnning " + _clients.length + " clients\n");
        ExecutorService executor = Executors.newFixedThreadPool(_clients.length, new DaemonThreadFactory());

        _barrier = new CyclicBarrier(_clients.length+1);

        for (int i = 0; i < _clients.length; i++) {
            FutureTask<RunResult> tmp = new FutureTask<RunResult>(_clients[i]);
            _running.put(new Integer(i), tmp);
            executor.execute(tmp);
        }

        System.out.print("Clients started\n");
        // Wait for clients to connect to their assigned server, and
        // start timer which ensures we have outstanding requests.
        LOG.info("Waiting for clients to connect");

        while (true) {
            try {
                Thread.sleep(1000);
                break;
            } catch (InterruptedException e) {
                continue;
            }
        }

        _startCpuTime = System.nanoTime();
        try {
            _barrier.await();
        } catch (BrokenBarrierException e) {
            LOG.warn("Some other client was interrupted; Benchmark main thread is out of sync", e);
        } catch (InterruptedException e) {
            LOG.warn("Benchmark main thread was interrupted while waiting on barrier", e);
        }

        System.out.print("Done waiting for connections\n");

        // Wait for the test to finish
        RunResult[] results = new RunResult[_clients.length];
        for (Integer i: _running.keySet()) {
            try {
                results[i] = _running.get(i).get();
            } catch (Exception e) {
                executor.shutdown();
                LOG.warn("Error in thread", e);
                throw new RuntimeException("Interrupted");
            }
        }

        System.out.print("client,duration,ops\n");
        double totalThroughputEst = 0;
        for (int i = 0; i < results.length; i++) {
            RunResult result = results[i];
            totalThroughputEst += (1.0*result.numOps) / (result.getDurationNanos()/1000000000.0);
            System.out.print("client-" + i + "," + result.getDurationNanos() + "," + result.numOps + "\n");
        }

        executor.shutdown();

        LOG.info("Test finished: operations: " + _totalOps + " avg rate: " +
                 totalThroughputEst);

        System.out.println("\n");
        System.out.println("clients,keys,throughput\n");
        System.out.println("" + getClients() + ","  + getKeys() + "," + totalThroughputEst);
    }

    int getClients() {
        return _clients.length;
    }

    int getTotalOps() {
        return _totalOps;
    }

    int getKeys() {
        return _keys;
    }

    CyclicBarrier getBarrier() {
        return _barrier;
    }

    String getData() {
        return _data;
    }

    int getInterval() {
        return _interval;
    }

    long getStartTime() {
        return _startCpuTime;
    }

    private static PropertiesConfiguration initConfiguration(String[] args) {
        OptionSet options = null;
        OptionParser parser = new OptionParser();
        PropertiesConfiguration conf = null;

        // Setup the option parser
        parser.accepts("help", "print this help statement");
        parser.accepts("conf", "configuration file (required)").
            withRequiredArg().ofType(String.class).required();
        parser.accepts("interval", "interval between rate measurements").
            withRequiredArg().ofType(Integer.class);
        parser.accepts("ops", "total number of operations").
            withRequiredArg().ofType(Integer.class);
        parser.accepts("lbound",
                       "lowerbound for the number of operations").
            withRequiredArg().ofType(Integer.class);
        parser.accepts("time", "time tests will run for (milliseconds)").
            withRequiredArg().ofType(Integer.class);
        parser.accepts("clients", "number of clients").
            withRequiredArg().ofType(Integer.class).required();
        parser.accepts("keys", "number of keys").
            withRequiredArg().ofType(Integer.class).required();

        // Parse and gather the arguments
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            System.out.println("\nError parsing arguments: " + e.getMessage() + "\n");
            try {
                parser.printHelpOn(System.out);
            } catch (IOException e2) {
                LOG.error("Exception while printing help message", e2);
            }
            System.exit(-1);
        }

        Integer interval = (Integer) options.valueOf("interval");
        Integer totOps = (Integer) options.valueOf("ops");
        Integer lowerbound = (Integer) options.valueOf("lbound");
        Integer time = (Integer) options.valueOf("time");
        Integer clients = (Integer) options.valueOf("clients");
        Integer keys = (Integer) options.valueOf("keys");

        // Load and parse the configuration file
        String configFile = (String) options.valueOf("conf");
        LOG.info("Loading benchmark from configuration file: " + configFile);

        try {
            conf = new PropertiesConfiguration(configFile);
        } catch (ConfigurationException e) {
            LOG.error("Failed to read configuration file: " + configFile, e);
            System.exit(-2);
        }

        // If there are options from command line, override the conf
        if (interval != null)
            conf.setProperty("interval", interval);
        if (totOps != null)
            conf.setProperty("totalOperations", totOps);
        if (lowerbound != null)
            conf.setProperty("lowerbound", lowerbound);

        conf.setProperty("clients", clients);
        conf.setProperty("keys", keys);

        return conf;
    }

    public static void main(String[] args) {

        // Parse command line and configuration file
        PropertiesConfiguration conf = initConfiguration(args);

        // Helpful info for users of our default log4j configuration
        Appender a = Logger.getRootLogger().getAppender("file");
        if (a != null && a instanceof FileAppender) {
            FileAppender fa = (FileAppender) a;
            System.out.println("Detailed logs going to: " + fa.getFile());
        }

        // Run the benchmark
        try {
            ZooKeeperBenchmark benchmark = new ZooKeeperBenchmark(conf);
            benchmark.runBenchmark();
        } catch (Exception e) {
            LOG.error("Error: ", e);
            System.out.println("Error: " + e + "\n");
            System.exit(1);
        }

        System.exit(0);
    }

}

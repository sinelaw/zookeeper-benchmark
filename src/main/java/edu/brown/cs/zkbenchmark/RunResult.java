package edu.brown.cs.zkbenchmark;

public class RunResult {
    public static class OpTime {
        public long startTime;
        public long endTime;
        public OpTime(long s, long e) {
            startTime = s;
            endTime = e;
        }
    };

    public long startNanos;
    public long endNanos;
    public long numOps;
    public long getDurationNanos() {
        return endNanos - startNanos;
    }
    public OpTime[] latencies;

    public long getAverageLatencyNanos() {
        long avgLatency = 0;
        for (int i = 0; i < latencies.length; i++) {
            avgLatency += (latencies[i].endTime - latencies[i].startTime) / latencies.length;
        }
        return avgLatency;
    }

    public long[] getLatenciesPerSecond() {
        int durationSecs = (int)(getDurationNanos() / (1000 * 1000 * 1000)) + 1;
        long[] avgLatencies = new long[durationSecs];
        long[] binSizes = new long[durationSecs];
        for (int i = 0; i < durationSecs; i++) {
            avgLatencies[i] = 0;
            binSizes[i] = 0;
        }
        for (int i = 0; i < latencies.length; i++) {
            int sec = (int)((latencies[i].startTime - startNanos) / (1000 * 1000 * 1000));
            avgLatencies[sec] += (latencies[i].endTime - latencies[i].startTime) / 1000; // micros
            binSizes[sec]++;
        }
        long[] results = new long[avgLatencies.length];
        for (int i = 0; i < avgLatencies.length; i++) {
            results[i] = binSizes[i] == 0 ? 0 : avgLatencies[i]/binSizes[i];
        }
        return results;
    }
}

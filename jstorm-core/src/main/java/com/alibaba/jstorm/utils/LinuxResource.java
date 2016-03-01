package com.alibaba.jstorm.utils;

import backtype.storm.utils.ShellUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dongbin on 2016/1/25.
 */
public class LinuxResource {
    private static final Logger LOG = LoggerFactory.getLogger(LinuxResource.class);

    public static final long JIFFY_LENGTH_IN_MILLIS;

    static {
        long jiffiesPerSecond = -1;
        try {
            ShellUtils.ShellCommandExecutor shellExecutorClk = new ShellUtils.ShellCommandExecutor(
                    new String[]{"getconf", "CLK_TCK"});
            shellExecutorClk.execute();
            jiffiesPerSecond = Long.parseLong(shellExecutorClk.getOutput().replace("\n", ""));
        } catch (IOException e) {
            LOG.error(JStormUtils.getErrorInfo(e));
        } finally {
            JIFFY_LENGTH_IN_MILLIS = jiffiesPerSecond != -1 ?
                    Math.round(1000D / jiffiesPerSecond) : -1;
        }
    }

    private static final String PROCFS_MEMFILE = "/proc/meminfo";

    private static final Pattern PROCFS_MEMFILE_FORMAT =
            Pattern.compile("^([a-zA-Z]*):[ \t]*([0-9]*)[ \t]kB");

    private static final String _MEMTOTAL = "MemTotal";
    private static final String _SWAPTOTAL = "SwapTotal";
    private static final String _MEMFREE = "MemFree";
    private static final String _SWAPFREE = "SwapFree";
    private static final String _INACTIVE = "Inactive";

    private static final String PROCFS_CPUINFO = "/proc/cpuinfo";

    private static final Pattern PROCESSOR_FORMAT =
            Pattern.compile("^processor[ \t]:[ \t]*([0-9]*)");

    private static final String PROCFS_STAT = "/proc/stat";

    private static final Pattern CPU_TIME_FORMAT =
            Pattern.compile("^cpu[ \t]*([0-9]*)" +
                    "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");

    private static String procfsMemFile = PROCFS_MEMFILE;
    private static String procfsCpuFile = PROCFS_CPUINFO;
    private static String procfsStatFile = PROCFS_STAT;
    private static long jiffyLengthInMillis = JIFFY_LENGTH_IN_MILLIS;
    private static CpuUsageCalculator cpuUsageCalculator = new CpuUsageCalculator(jiffyLengthInMillis);
    ;

    private static long ramSize = 0;
    private static long swapSize = 0;
    private static long ramSizeFree = 0;  // free ram (kB)
    private static long swapSizeFree = 0; // free swap(kB)
    private static long inactiveSize = 0; // inactive cache memory (kB)
    private static int numProcessors = 0;

    static boolean readMemInfoFile = false;
    static boolean readCpuInfoFile = false;

    private static void readProcMemInfoFile(boolean readAgain) {
        if (readMemInfoFile && !readAgain) {
            return;
        }
        FileParse fileParse = new FileParse(procfsMemFile, new LineParse() {
            @Override
            public void prepare() {
            }

            @Override
            public boolean parseLine(String line) {
                Matcher mat = PROCFS_MEMFILE_FORMAT.matcher(line);
                if (mat.find()) {
                    if (mat.group(1).equals(_MEMTOTAL)) {
                        ramSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(_SWAPTOTAL)) {
                        swapSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(_MEMFREE)) {
                        ramSizeFree = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(_SWAPFREE)) {
                        swapSizeFree = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(_INACTIVE)) {
                        inactiveSize = Long.parseLong(mat.group(2));
                    }
                }
                return false;
            }
        });
        fileParse.parse();
        readMemInfoFile = true;
    }

    private static void readProcCpuInfoFile() {
        // This directory needs to be read only once
        if (readCpuInfoFile) {
            return;
        }
        FileParse fileParse = new FileParse(procfsCpuFile, new LineParse() {
            @Override
            public void prepare() {
                numProcessors = 0;
            }

            @Override
            public boolean parseLine(String line) {
                Matcher mat = PROCESSOR_FORMAT.matcher(line);
                if (mat.find()) {
                    numProcessors++;
                }
                return false;
            }
        });
        fileParse.parse();
        readCpuInfoFile = true;
    }


    private static void readProcStatFile() {
        FileParse fileParse = new FileParse(procfsStatFile, new LineParse() {
            @Override
            public void prepare() {
            }

            @Override
            public boolean parseLine(String line) {
                Matcher mat = CPU_TIME_FORMAT.matcher(line);
                if (mat.find()) {
                    long uTime = Long.parseLong(mat.group(1));
                    long nTime = Long.parseLong(mat.group(2));
                    long sTime = Long.parseLong(mat.group(3));
                    cpuUsageCalculator.updateElapsedJiffies(
                            BigInteger.valueOf(uTime + nTime + sTime),
                            getCurrentTime());
                    return true;
                }
                return false;
            }
        });
        fileParse.parse();
    }

    static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    private static void readProcMemInfoFile() {
        readProcMemInfoFile(false);
    }

    public static long getPhysicalMemorySize() {
        readProcMemInfoFile();
        return ramSize * 1024;
    }

    public static long getVirtualMemorySize() {
        readProcMemInfoFile();
        return (ramSize + swapSize) * 1024;
    }

    public static long getFreePhysicalMemorySize() {
        readProcMemInfoFile(true);
        return (ramSizeFree + inactiveSize) * 1024;
    }

    public static long getFreeVirtualMemorySize() {
        readProcMemInfoFile(true);
        return (ramSizeFree + swapSizeFree + inactiveSize) * 1024;
    }

    public static int getNumProcessors() {
        readProcCpuInfoFile();
        return numProcessors;
    }

    public static float getCpuUsage() {
        readProcStatFile();
        float overallCpuUsage = cpuUsageCalculator.getCpuTrackerUsagePercent();
        if (overallCpuUsage != -1) {
            overallCpuUsage = overallCpuUsage / getNumProcessors();
        }
        return overallCpuUsage;
    }

    public abstract static class LineParse {
        public abstract void prepare();

        public abstract boolean parseLine(String line);
    }

    static class FileParse {
        static LineParse parse;
        static String file;

        public FileParse(String file, LineParse parse) {
            this.parse = parse;
            this.file = file;
            parse.prepare();
        }

        public static void parse() {
            BufferedReader in = null;
            InputStreamReader fReader = null;
            try {
                fReader = new InputStreamReader(
                        new FileInputStream(file), Charset.forName("UTF-8"));
                in = new BufferedReader(fReader);
            } catch (FileNotFoundException f) {
                return;
            }
            try {
                String str = in.readLine();
                while (str != null) {
                    if (parse.parseLine(str))
                        break;
                    str = in.readLine();
                }
            } catch (IOException io) {
                LOG.warn("Error reading the stream " + io);
            } finally {
                try {
                    fReader.close();
                    try {
                        in.close();
                    } catch (IOException i) {
                        LOG.warn("Error closing the stream " + in);
                    }
                } catch (IOException i) {
                    LOG.warn("Error closing the stream " + fReader);
                }
            }
        }
    }

    static class CpuUsageCalculator {

        static BigInteger systemCpuTime = BigInteger.ZERO;

        static BigInteger lastSystemCpuTime = BigInteger.ZERO;

        static BigInteger jiffyLengthInMillis;

        static long sampleTime;
        static long lastSampleTime;
        static float cpuUsage;

        static long MINIMUM_UPDATE_INTERVAL;

        public CpuUsageCalculator(long jiffyLengthInMillis) {
            this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
            this.cpuUsage = -1;
            this.sampleTime = -1;
            this.lastSampleTime = -1;
            this.MINIMUM_UPDATE_INTERVAL = 10 * jiffyLengthInMillis;
        }

        public float getCpuTrackerUsagePercent() {
            if (lastSampleTime == -1 ||
                    lastSampleTime > sampleTime) {

                lastSampleTime = sampleTime;
                lastSystemCpuTime = systemCpuTime;
                return cpuUsage;//return -1 first time
            }
            if (sampleTime > lastSampleTime + MINIMUM_UPDATE_INTERVAL) {

                cpuUsage =
                        ((systemCpuTime.subtract(lastSystemCpuTime)).floatValue())
                                * 100F / ((float) (sampleTime - lastSampleTime));
                lastSampleTime = sampleTime;
                lastSystemCpuTime = systemCpuTime;
            }
            return cpuUsage;
        }

        public void updateElapsedJiffies(BigInteger elapedJiffies, long sampleTime) {
            this.systemCpuTime = elapedJiffies.multiply(jiffyLengthInMillis);
            this.sampleTime = sampleTime;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(5000);
            System.out.println(LinuxResource.getCpuUsage());
            System.out.println(LinuxResource.getFreePhysicalMemorySize());
        }
    }
}
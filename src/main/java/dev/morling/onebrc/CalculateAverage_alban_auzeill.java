/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

public final class CalculateAverage_alban_auzeill {

    private static final String FILE = "./measurements.txt";

    public static final int READ_BUFFER_SIZE = 16 * 1024 * 1024;

    public static void main(String[] args) throws IOException, InterruptedException {
        var out = new BufferedOutputStream(System.out, 16 * 1024);
        execute(Paths.get(FILE), out);
        out.flush();
    }

    static final class Parser extends Thread {
        final SynchronizedContext context;

        public Parser(SynchronizedContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {
                StationDatabase database = new StationDatabase();
                byte[] data = new byte[READ_BUFFER_SIZE];
                var time1 = System.nanoTime();
                int dataIndexEnd = context.read(data);
                var time2 = System.nanoTime();
                context.timeToRead.addAndGet(time2 - time1);
                while (dataIndexEnd > 0) {
                    long time3 = System.nanoTime();
                    int dataIndex = 0;
                    int currentNode = StationDatabase.ROOT_NODE;
                    boolean stateStation = true;
                    int temperature = 0;
                    boolean negative = false;
                    while (dataIndex < dataIndexEnd) {
                        byte ch = data[dataIndex];
                        dataIndex++;
                        if (stateStation) {
                            if (ch == ';') {
                                stateStation = false;
                                temperature = 0;
                                negative = false;
                            }
                            else {
                                currentNode = database.nodeIndex(currentNode, ch);
                            }
                        }
                        else {
                            if (ch == '\n' || dataIndex == dataIndexEnd) {
                                database.add(currentNode, negative ? -temperature : temperature);
                                stateStation = true;
                                currentNode = StationDatabase.ROOT_NODE;
                            }
                            else if (ch == '-') {
                                negative = true;
                            }
                            else if (ch != '.') {
                                temperature = temperature * 10 + ch - '0';
                            }
                        }
                    }
                    long time4 = System.nanoTime();
                    dataIndexEnd = context.read(data);
                    long time5 = System.nanoTime();
                    context.timeToConvert.addAndGet(time4 - time3);
                    context.timeToRead.addAndGet(time5 - time4);
                }
                long time6 = System.nanoTime();
                context.merge(database);
                long time7 = System.nanoTime();
                context.timeToMerge.addAndGet(time7 - time6);
            }
            catch (IOException ex) {
                context.saveException(ex);
            }
        }
    }

  public static void execute(Path inputPath, OutputStream out) throws IOException, InterruptedException {
        int threadCount = Math.max(2, Runtime.getRuntime().availableProcessors() - 1);
        try (var input = Files.newInputStream(inputPath)) {
            var context = new SynchronizedContext(input);
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Parser(context);
                threads[i].start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }
            if (context.exception != null) {
                throw context.exception;
            }
            long time1 = System.nanoTime();
            context.mergedDatabase.write(out);
            long time2 = System.nanoTime();
            System.err.println(STR."timeToRead: \{context.timeToRead.get() / 1_000_000.0d}ms");
            System.err.println(STR."timeToConvert \{context.timeToConvert.get() / 1_000_000.0d}ms");
            System.err.println(STR."timeToMerge \{context.timeToMerge.get() / 1_000_000.0d}ms");
            System.err.println(STR."timeToPrint: \{(time2 - time1) / 1_000_000.0d}ms");
        }
    }

    static final class SynchronizedContext {
        final InputStream input;
        byte[] previous = new byte[1024];
        int previousEnd = 0;
        final StationDatabase mergedDatabase;
        IOException exception;
        AtomicLong timeToRead = new AtomicLong();
        AtomicLong timeToConvert = new AtomicLong();
        AtomicLong timeToMerge = new AtomicLong();

        public SynchronizedContext(InputStream input) {
            this.input = input;
            mergedDatabase = new StationDatabase();
        }

        int read(byte[] data) throws IOException {
            if (exception != null) {
                return 0;
            }
            synchronized (input) {
                if (previousEnd > 0) {
                    System.arraycopy(previous, 0, data, 0, previousEnd);
                }
                int read = input.read(data, previousEnd, data.length - previousEnd);
                int end = read == -1 ? previousEnd : previousEnd + read;
                if (end == data.length) {
                    int newLineIndex = end - 1;
                    while (data[newLineIndex] != '\n') {
                        newLineIndex--;
                    }
                    previousEnd = end - newLineIndex - 1;
                    System.arraycopy(data, newLineIndex + 1, previous, 0, previousEnd);
                    end = newLineIndex + 1;
                }
                else {
                    previousEnd = 0;
                }
                return end;
            }
        }

        void merge(StationDatabase database) {
            synchronized (mergedDatabase) {
                mergedDatabase.merge(database);
            }
        }

        synchronized void saveException(IOException ex) {
            if (exception == null) {
                exception = ex;
            }
        }
    }

    static final class StationData {
        int min;
        int max;
        long sum;
        long count;

        StationData(int temperature) {
            min = temperature;
            max = temperature;
            sum = temperature;
            count = 1;
        }

        StationData(StationData other) {
            min = other.min;
            max = other.max;
            sum = other.sum;
            count = other.count;
        }

        int mean() {
            int roundIncrement = sum >= 0 ? 5 : -5;
            return (int) (((sum * 10 / count) + roundIncrement) / 10);
        }

        void add(int temperature) {
            min = (min <= temperature) ? min : temperature;
            max = (max >= temperature) ? max : temperature;
            sum += temperature;
            count++;
        }

        void merge(StationData other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }
    }

    static final class StationDatabase {
        private static final int ROOT_NODE = 0;
        private static final int NODE_SIZE = 1 /* children level 1 indexes */ +
                1 /* stationDataIndex */ +
                1 /* ch */ +
                1 /* parent index */;
        private static final int STATION_INDEX_OFFSET = 1;
        private static final int CH_INDEX_OFFSET = 2;
        private static final int PARENT_INDEX_OFFSET = 3;

        private static final int CHILDREN_HIGH_BITS_INDEXES = 16;
        private static final int CHILDREN_LOW_BITS_INDEXES = 16;

        StationData[] stations = new StationData[1000];
        int nextAvailableStation = 1;
        int[] stationIndexes = new int[16000];
        int nextAvailableIndex = NODE_SIZE;

        void writeName(OutputStream out, int nodeIndex) throws IOException {
            int ch = stationIndexes[nodeIndex + CH_INDEX_OFFSET];
            if (ch == 0) {
                return;
            }
            writeName(out, stationIndexes[nodeIndex + PARENT_INDEX_OFFSET]);
            out.write((byte) ch);
        }

        static class Separator {
            boolean first = true;

            void write(OutputStream out) throws IOException {
                if (first) {
                    first = false;
                }
                else {
                    out.write(',');
                    out.write(' ');
                }
            }
        }

        void write(OutputStream out) throws IOException {
            out.write('{');
            write(out, ROOT_NODE, new Separator());
            out.write('}');
            out.write('\n');
        }

        void write(OutputStream out, int nodeIndex, Separator separator) throws IOException {
            int stationIndex = stationIndexes[nodeIndex + STATION_INDEX_OFFSET];
            if (stationIndex != 0) {
                StationData stationData = stations[stationIndex];
                separator.write(out);
                writeName(out, nodeIndex);
                out.write('=');
                writeFixPoint(out, stationData.min);
                out.write('/');
                writeFixPoint(out, stationData.mean());
                out.write('/');
                writeFixPoint(out, stationData.max);
            }
            forEachChildNode(nodeIndex, childNodeIndex -> {
                try {
                    write(out, childNodeIndex, separator);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        static void writeFixPoint(OutputStream out, int value) throws IOException {
            if (value < 0) {
                out.write('-');
                value = -value;
            }
            if (value >= 100) {
                out.write('0' + (value / 100));
                value %= 100;
            }
            out.write('0' + (value / 10));
            out.write('.');
            out.write('0' + (value % 10));
        }

        int allocStation() {
            int index = nextAvailableStation;
            nextAvailableStation++;
            if (index >= stations.length) {
                var biggerArray = new StationData[stations.length * 2];
                System.arraycopy(stations, 0, biggerArray, 0, stations.length);
                stations = biggerArray;
            }
            return index;
        }

        int allocStationIndexes(int size) {
            int index = nextAvailableIndex;
            nextAvailableIndex += size;
            if (nextAvailableIndex > stationIndexes.length) {
                var biggerArray = new int[stationIndexes.length * 2];
                System.arraycopy(stationIndexes, 0, biggerArray, 0, stationIndexes.length);
                stationIndexes = biggerArray;
            }
            return index;
        }

        int nodeIndex(int parentNodeIndex, byte ch) {
            int childrenLevel1Indexes = stationIndexes[parentNodeIndex];
            if (childrenLevel1Indexes == 0) {
                childrenLevel1Indexes = allocStationIndexes(CHILDREN_HIGH_BITS_INDEXES);
                stationIndexes[parentNodeIndex] = childrenLevel1Indexes;
            }
            int level1 = (ch & 0xF0) >> 4;
            int level2 = ch & 0xF;
            int childrenLevel1Index = childrenLevel1Indexes + level1;
            int childrenNodeLevel2Indexes = stationIndexes[childrenLevel1Index];
            if (childrenNodeLevel2Indexes == 0) {
                childrenNodeLevel2Indexes = allocStationIndexes(CHILDREN_LOW_BITS_INDEXES);
                stationIndexes[childrenLevel1Index] = childrenNodeLevel2Indexes;
            }
            int childrenLevel2Index = childrenNodeLevel2Indexes + level2;
            int nodeIndex = stationIndexes[childrenLevel2Index];
            if (nodeIndex == 0) {
                nodeIndex = allocStationIndexes(NODE_SIZE);
                stationIndexes[childrenLevel2Index] = nodeIndex;
                stationIndexes[nodeIndex + CH_INDEX_OFFSET] = (ch & 0xFF);
                stationIndexes[nodeIndex + PARENT_INDEX_OFFSET] = parentNodeIndex;
            }
            return nodeIndex;
        }

        void add(int nodeIndex, int temperature) {
            int stationIndex = stationIndexes[nodeIndex + STATION_INDEX_OFFSET];
            if (stationIndex == 0) {
                stationIndex = allocStation();
                stationIndexes[nodeIndex + STATION_INDEX_OFFSET] = stationIndex;
            }
            StationData stationData = stations[stationIndex];
            if (stationData == null) {
                stationData = new StationData(temperature);
                stations[stationIndex] = stationData;
            }
            else {
                stationData.add(temperature);
            }
        }

        void forEachChildNode(int parentNodeIndex, IntConsumer consumer) {
            int childrenLevel1Indexes = stationIndexes[parentNodeIndex];
            if (childrenLevel1Indexes != 0) {
                for (int i = 0; i < CHILDREN_HIGH_BITS_INDEXES; i++) {
                    int childrenLevel1Index = childrenLevel1Indexes + i;
                    int childrenNodeLevel2Indexes = stationIndexes[childrenLevel1Index];
                    if (childrenNodeLevel2Indexes != 0) {
                        for (int j = 0; j < CHILDREN_LOW_BITS_INDEXES; j++) {
                            int childrenLevel2Index = childrenNodeLevel2Indexes + j;
                            int nodeIndex = stationIndexes[childrenLevel2Index];
                            if (nodeIndex != 0) {
                                consumer.accept(nodeIndex);
                            }
                        }
                    }
                }
            }
        }

        void merge(StationDatabase other) {
            merge(other, ROOT_NODE, ROOT_NODE);
        }

        void merge(StationDatabase other, int thisNodeIndex, int otherNodeIndex) {
            int thisStationIndex = stationIndexes[thisNodeIndex + STATION_INDEX_OFFSET];
            int otherStationIndex = other.stationIndexes[otherNodeIndex + STATION_INDEX_OFFSET];
            if (thisStationIndex != 0 && otherStationIndex != 0) {
                stations[thisStationIndex].merge(other.stations[otherStationIndex]);
            }
            else if (otherStationIndex != 0) {
                int stationIndex = allocStation();
                stationIndexes[thisNodeIndex + STATION_INDEX_OFFSET] = stationIndex;
                stations[stationIndex] = new StationData(other.stations[otherStationIndex]);
            }
            other.forEachChildNode(otherNodeIndex, otherChildNodeIndex -> {
                int ch = other.stationIndexes[otherChildNodeIndex + CH_INDEX_OFFSET];
                int thisChildNodeIndex = nodeIndex(thisNodeIndex, (byte) ch);
                merge(other, thisChildNodeIndex, otherChildNodeIndex);
            });
        }

    }

}

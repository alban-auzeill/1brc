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
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import sun.misc.Unsafe;

public class CalculateAverage_alban_auzeill {

    private static final String FILE = "./measurements.txt";

    private static final int MIN_CHUNK_SIZE = 64 * 1024;

    private static final Unsafe UNSAFE;

    static {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            UNSAFE = (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (Arrays.asList(args).contains("--worker")) {
            spawn();
        }
        else {
            execute();
        }
    }

    private static void spawn() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> commands = new ArrayList<>();
        info.command().ifPresent(commands::add);
        info.arguments().ifPresent(args -> commands.addAll(Arrays.asList(args)));
        commands.add("--worker");

        new ProcessBuilder()
                .command(commands)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    private static void execute() throws IOException, InterruptedException {
        var out = new BufferedOutputStream(System.out, 16 * 1024);
        execute(Paths.get(FILE), out);
        out.flush();
    }

    public static void execute(Path inputPath, OutputStream out) throws IOException, InterruptedException {
        try (var fileChannel = FileChannel.open(inputPath, StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long maxNumberOfChunk = Math.max(1, fileSize / MIN_CHUNK_SIZE);
            int threadCount = (int) Math.min(maxNumberOfChunk, Math.max(1, Runtime.getRuntime().availableProcessors()));
            var memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            long fileAddress = memorySegment.address();
            long chunkStart = 0;
            long chunkSize = (fileSize / threadCount) + 1;
            AtomicReference<RuntimeException> firstException = new AtomicReference<>();
            Thread[] threads = new Thread[threadCount];
            Node mergedResult = new Node(null, (byte) 0);
            for (int i = 0; i < threadCount; i++) {
                long chunkEnd = Math.min(chunkStart + chunkSize, fileSize);
                threads[i] = new Parser(fileAddress, fileSize, chunkStart, chunkEnd, mergedResult, firstException);
                threads[i].start();
                chunkStart = chunkEnd;
            }
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
            }
            var runtimeException = firstException.get();
            if (runtimeException != null) {
                throw runtimeException;
            }
            out.write('{');
            mergedResult.write(out);
            out.write('}');
            out.write('\n');
        }
    }

    static class Parser extends Thread {
        private final long fileAddress;
        private final long fileSize;
        private final long chunkStart;
        private final long chunkEnd;
        private final Node mergedResult;
        private final AtomicReference<RuntimeException> firstException;

        public Parser(long fileAddress, long fileSize, long chunkStart, long chunkEnd, Node mergedResult, AtomicReference<RuntimeException> firstException) {
            this.fileAddress = fileAddress;
            this.fileSize = fileSize;
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
            this.mergedResult = mergedResult;
            this.firstException = firstException;
        }

        @Override
        public void run() {
            try {
                Node root = new Node(null, (byte) 0);
                Node current = root;
                boolean stateStation = true;
                int temperature = 0;
                boolean negative = false;
                long memoryOffset = chunkStart;
                long maxOffset = fileSize;

                // except for the first chunk, skip the end of the potential previous line
                if (chunkStart > 0) {
                    while (memoryOffset < chunkEnd) {
                        byte ch = UNSAFE.getByte(fileAddress + memoryOffset);
                        memoryOffset++;
                        if (ch == '\n') {
                            break;
                        }
                    }
                }
                while (memoryOffset < maxOffset) {
                    byte ch = UNSAFE.getByte(fileAddress + memoryOffset);
                    if (stateStation) {
                        if (ch == ';') {
                            stateStation = false;
                            temperature = 0;
                            negative = false;
                        }
                        else {
                            current = current.get(ch);
                        }
                    }
                    else {
                        if (ch == '\n') {
                            current.add(negative ? -temperature : temperature);
                            stateStation = true;
                            current = root;
                            if (memoryOffset >= chunkEnd) {
                                break;
                            }
                        }
                        else if (ch == '-') {
                            negative = true;
                        }
                        else if (ch != '.') {
                            temperature = temperature * 10 + ch - '0';
                        }
                    }
                    memoryOffset++;
                }
                // Not mandatory, but try to support if the file does not end with a new line
                if (current != root && !stateStation) {
                    current.add(negative ? -temperature : temperature);
                }
                synchronized (mergedResult) {
                    mergedResult.merge(root);
                }
            }
            catch (RuntimeException ex) {
                firstException.compareAndSet(null, ex);
            }
        }
    }

    static class Node {
        Node parent;
        byte ch;
        Node[] childrenByCh;
        Node[] sortedChildren;
        int min;
        int max;
        long sum;
        long count;

        Node(Node parent, byte ch) {
            this.parent = parent;
            this.ch = ch;
        }

        void merge(Node other) {
            if (other.count > 0) {
                if (count == 0) {
                    min = other.min;
                    max = other.max;
                }
                else {
                    min = Math.min(min, other.min);
                    max = Math.max(max, other.max);
                }
                sum += other.sum;
                count += other.count;
            }
            if (other.sortedChildren != null) {
                Node otherChild;
                for (int i = 0; (otherChild = other.sortedChildren[i]) != null; i++) {
                    get(otherChild.ch).merge(otherChild);
                }
            }
        }

        int rank() {
            return ch & 0xff;
        }

        Node get(byte ch) {
            if (childrenByCh == null) {
                childrenByCh = new Node[256];
                sortedChildren = new Node[256];
            }
            Node child = childrenByCh[ch & 0xff];
            if (child == null) {
                child = new Node(this, ch);
                childrenByCh[ch & 0xff] = child;
                int i = 0;
                int rank = child.rank();
                while (sortedChildren[i] != null && sortedChildren[i].rank() < rank) {
                    i++;
                }
                Node current = sortedChildren[i];
                sortedChildren[i] = child;
                i++;
                while (current != null) {
                    Node next = sortedChildren[i];
                    sortedChildren[i] = current;
                    current = next;
                    i++;
                }
            }
            return child;
        }

        void add(int temperature) {
            if (count == 0) {
                min = temperature;
                max = temperature;

            }
            else {
                min = Math.min(min, temperature);
                max = Math.max(max, temperature);
            }
            sum += temperature;
            count++;
        }

        void writeName(OutputStream out) throws IOException {
            if (ch == 0) {
                return;
            }
            parent.writeName(out);
            out.write(ch);
        }

        void write(OutputStream out) throws IOException {
            if (count > 0) {
                writeName(out);
                out.write('=');
                writeFixPoint(out, min);
                out.write('/');
                writeFixPoint(out, mean());
                out.write('/');
                writeFixPoint(out, max);
            }
            if (sortedChildren != null) {
                if (count > 0) {
                    out.write(',');
                    out.write(' ');
                }
                Node child;
                for (int i = 0; (child = sortedChildren[i]) != null; i++) {
                    if (i != 0) {
                        out.write(',');
                        out.write(' ');
                    }
                    child.write(out);
                }
            }
        }

        int mean() {
            int roundIncrement = sum >= 0 ? 5 : -5;
            return (int) (((sum * 10 / count) + roundIncrement) / 10);
        }

        void writeFixPoint(OutputStream out, int value) throws IOException {
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
    }

}

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

public class CalculateAverage_alban_auzeill {

    private static final String FILE = "./measurements.txt";

    public static final int READ_BUFFER_SIZE = 12 * 1024 * 1024;

    public static void main(String[] args) throws IOException, InterruptedException {
        var out = new BufferedOutputStream(System.out, 16 * 1024);
        average(Paths.get(FILE), out);
        out.flush();
    }

    static class Parser extends Thread {
        final SynchronizedContext context;

        public Parser(SynchronizedContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {
                byte[] data = new byte[READ_BUFFER_SIZE];
                int dataIndexEnd;
                while ((dataIndexEnd = context.read(data)) > 0) {
                    int dataIndex = 0;
                    Node root = new Node(null, (byte) 0);
                    Node current = root;
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
                                current = current.get(ch);
                            }
                        }
                        else {
                            if (ch == '\n' || dataIndex == dataIndexEnd) {
                                current.add(negative ? -temperature : temperature);
                                stateStation = true;
                                current = root;
                            }
                            else if (ch == '-') {
                                negative = true;
                            }
                            else if (ch != '.') {
                                temperature = temperature * 10 + ch - '0';
                            }
                        }
                    }
                    context.merge(root);
                }
            }
            catch (IOException ex) {
                context.saveException(ex);
            }
        }
    }

    public static void average(Path inputPath, OutputStream out) throws IOException, InterruptedException {
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
            out.write('{');
            context.result.write(out);
            out.write('}');
            out.write('\n');
        }
    }

    static class SynchronizedContext {
        InputStream input;
        byte[] previous = new byte[READ_BUFFER_SIZE];
        int previousEnd = 0;
        Node result;
        IOException exception;

        public SynchronizedContext(InputStream input) {
            this.input = input;
            result = new Node(null, (byte) 0);
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

        void merge(Node root) {
            synchronized (result) {
                result.merge(root);
            }
        }

        synchronized void saveException(IOException ex) {
            if (exception == null) {
                exception = ex;
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

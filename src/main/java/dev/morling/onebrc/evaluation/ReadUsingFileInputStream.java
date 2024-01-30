package dev.morling.onebrc.evaluation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class ReadUsingFileInputStream {

  public static long countByteInFileWithBuffer(Path path, int bufferSize) throws IOException {
    long size = 0;
    try (InputStream input = Files.newInputStream(path)) {
      byte[] buffer = new byte[bufferSize];
      int read = input.read(buffer);
      while (read != -1) {
        size += read;
        read = input.read(buffer);
      }
    }
    return size;
  }

  public static long countNewLineWithBuffer(Path path, int bufferSize) throws IOException {
    long count = 0;
    try (InputStream input = Files.newInputStream(path)) {
      byte[] buffer = new byte[bufferSize];
      int read = input.read(buffer);
      while (read != -1) {
        for (int i = 0; i < read; i++) {
          if (buffer[i] == '\n') {
            count++;
          }
        }
        read = input.read(buffer);
      }
    }
    return count;
  }

  public static long countNewLineInFileUsingThreadsWithBuffer(Path path, int threadCount) throws IOException, InterruptedException {
    var fileSize = Files.size(path);
    var chunkSize = (fileSize / threadCount) + 1L;
    var exceptions = new ConcurrentLinkedDeque<Throwable>();
    AtomicLong totalNewLineCount = new AtomicLong(0L);
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      long start = Math.min(chunkSize * i, fileSize);
      long end = Math.min(chunkSize * (i + 1), fileSize);
      threads[i] = new Thread(() -> {
        try {
          try (InputStream input = Files.newInputStream(path)) {
            long nbNewLineCount = 0;
            input.skip(start);
            long remaining = end - start;
            byte[] buffer = new byte[256 * 1024];
            int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            while (read > 0) {
              for (int j = 0; j < read; j++) {
                if (buffer[j] == '\n') {
                  nbNewLineCount++;
                }
              }
              remaining -= read;
              read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            }
            totalNewLineCount.addAndGet(nbNewLineCount);
          }
        } catch (IOException | RuntimeException e) {
          exceptions.add(e);
        }
      });
      threads[i].start();
    }
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    if (!exceptions.isEmpty()) {
      throw new RuntimeException(exceptions.getFirst());
    }
    return totalNewLineCount.get();
  }

}

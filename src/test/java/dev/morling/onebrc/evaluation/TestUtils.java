package dev.morling.onebrc.evaluation;

import java.util.concurrent.Callable;

public class TestUtils {

  public static <T> T time(Callable<T> callable, String message) {
    try {
      long start = System.nanoTime();
      T result = callable.call();
      long end = System.nanoTime();
      System.out.println(STR." Time: \{(end - start) / 1_000_000.0d}ms, \{message}");
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}

package dev.morling.onebrc.evaluation;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static dev.morling.onebrc.evaluation.ReadUsingFileInputStream.countByteInFileWithBuffer;
import static dev.morling.onebrc.evaluation.ReadUsingFileInputStream.countNewLineInFileUsingThreadsWithBuffer;
import static dev.morling.onebrc.evaluation.ReadUsingFileInputStream.countNewLineWithBuffer;
import static dev.morling.onebrc.evaluation.TestUtils.time;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadUsingFileInputStreamTest {

  /*
    Need to generate using ./create_measurements.sh 100000000 :
    measurements.txt
    measurements.out
    src/test/resources/samples/measurements-big.out
    src/test/resources/samples/measurements-big.txt
   */

  private static final Path FILE_13G = Path.of("./measurements.txt");

  private static final Path FILE_1_3G = Path.of("./src/test/resources/samples/measurements-big.txt");

  @ParameterizedTest
  @ValueSource(ints = {1 << 27, 1 << 27, 1 << 26, 1 << 25, 1 << 24, 1 << 23, 1 << 22, 1 << 21, 1 << 20, 1 << 19, 1 << 18, 1 << 17,
    1 << 16, 1 << 15, 1 << 14, 1 << 13, 1 << 12, 1 << 11, 1 << 10, 1 << 9, 1 << 8})
  void count_bytes_in_a_13G_file_with_a_buffer(int bufferSize) throws IOException {
    assertEquals(13795277638L,
      time(() -> countByteInFileWithBuffer(FILE_13G, bufferSize),
        STR."count bytes in a 13G file with a buffer of \{bufferSize} bytes"));
    /*
      M2:
      Time:  2'249.28ms buffer 134'217'728 bytes
      Time:  2'226.60ms buffer 134'217'728 bytes
      Time:  2'129.32ms buffer  67'108'864 bytes
      Time:  2'184.78ms buffer  33'554'432 bytes
      Time:  2'154.51ms buffer  16'777'216 bytes
      Time:  2'168.13ms buffer   8'388'608 bytes
      Time:  2'110.59ms buffer   4'194'304 bytes
      Time:  2'152.99ms buffer   2'097'152 bytes
      Time:  2'200.94ms buffer   1'048'576 bytes
      Time:  2'225.32ms buffer     524'288 bytes
      Time:  2'225.15ms buffer     262'144 bytes
      Time:  2'269.58ms buffer     131'072 bytes
      Time:  2'298.93ms buffer      65'536 bytes
      Time:  2'328.46ms buffer      32'768 bytes
      Time:  2'508.09ms buffer      16'384 bytes
      Time:  3'014.35ms buffer       8'192 bytes
      Time:  3'651.41ms buffer       4'096 bytes
      Time:  5'325.94ms buffer       2'048 bytes
      Time:  8'379.70ms buffer       1'024 bytes
      Time: 14'265.26ms buffer         512 bytes
      Time: 26'593.28ms buffer         256 bytes
      M1:
      Time:  3'187.02ms buffer 134'217'728 bytes
      Time:  2'494.26ms buffer 134'217'728 bytes
      Time:  2'365.16ms buffer  67'108'864 bytes
      Time:  2'398.87ms buffer  33'554'432 bytes
      Time:  2'404.20ms buffer  16'777'216 bytes
      Time:  2'394.71ms buffer   8'388'608 bytes
      Time:  2'158.07ms buffer   4'194'304 bytes
      Time:  2'222.46ms buffer   2'097'152 bytes
      Time:  2'282.18ms buffer   1'048'576 bytes
      Time:  2'234.40ms buffer     524'288 bytes
      Time:  2'243.17ms buffer     262'144 bytes
      Time:  2'248.81ms buffer     131'072 bytes
      Time:  2'251.46ms buffer      65'536 bytes
      Time:  2'397.47ms buffer      32'768 bytes
      Time:  2'986.67ms buffer      16'384 bytes
      Time:  3'014.61ms buffer       8'192 bytes
      Time:  3'831.32ms buffer       4'096 bytes
      Time:  5'494.90ms buffer       2'048 bytes
      Time:  9'073.00ms buffer       1'024 bytes
      Time: 16'387.94ms buffer         512 bytes
      Time: 30'835.59ms buffer         256 bytes
     */
  }

  @ParameterizedTest
  @ValueSource(ints = {1 << 27, 1 << 27, 1 << 26, 1 << 25, 1 << 24, 1 << 23, 1 << 22, 1 << 21, 1 << 20, 1 << 19, 1 << 18, 1 << 17,
    1 << 16, 1 << 15, 1 << 14, 1 << 13, 1 << 12, 1 << 11, 1 << 10, 1 << 9, 1 << 8})
  void count_new_line_in_a_13G_file_with_a_buffer(int bufferSize) throws IOException {
    assertEquals(1000000000L,
      time(() -> countNewLineWithBuffer(FILE_13G, bufferSize),
        STR."count new line in a 13G file with a buffer of \{bufferSize} bytes"));
    /*
      Time: 14'673.37ms buffer 134217728 bytes
      Time: 15'331.35ms buffer 134217728 bytes
      Time: 15'366.53ms buffer  67108864 bytes
      Time: 15'346.20ms buffer  33554432 bytes
      Time: 15'462.35ms buffer  16777216 bytes
      Time: 15'331.06ms buffer   8388608 bytes
      Time: 15'254.78ms buffer   4194304 bytes
      Time: 10'937.75ms buffer   2097152 bytes
      Time: 10'921.82ms buffer   1048576 bytes
      Time: 10'913.95ms buffer    524288 bytes
      Time: 10'924.88ms buffer    262144 bytes
      Time: 11'181.14ms buffer    131072 bytes
      Time: 11'412.20ms buffer     65536 bytes
      Time: 11'747.78ms buffer     32768 bytes
      Time: 12'070.38ms buffer     16384 bytes
      Time: 12'444.50ms buffer      8192 bytes
      Time: 13'119.68ms buffer      4096 bytes
      Time: 14'631.83ms buffer      2048 bytes
      Time: 21'044.75ms buffer      1024 bytes
      Time: 27'524.66ms buffer       512 bytes
      Time: 40'007.92ms buffer       256 bytes
    */
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 7, 6, 5, 4, 3, 2, 1})
  void count_new_lines_in_a_13G_file_with_thread_and_buffer(int threadCount) throws IOException {
    assertEquals(1000000000L,
      time(() -> countNewLineInFileUsingThreadsWithBuffer(FILE_13G, threadCount),
        STR."count bytes in a 13G file using \{threadCount} threads"));
    /*
     Time: 1'219.81ms 8 threads
     Time: 1'174.19ms 7 threads
     Time: 1'489.75ms 6 threads
     Time: 1'451.98ms 5 threads
     Time: 1'476.30ms 4 threads
     Time: 1'785.41ms 3 threads
     Time: 1'782.97ms 2 threads
     Time: 1'843.10ms 1 threads
     */
  }

}

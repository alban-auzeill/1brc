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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CalculateAverage_alban_auzeillTest {

  @ParameterizedTest
  @ValueSource(strings = {"1", "10", "10000-unique-keys", "2", "20", "3", "big", "boundaries", "complex-utf8",
    "dot", "rounding", "short", "shortest"})
  void test(String name) throws IOException, InterruptedException {
    Path inputFile = Paths.get(STR."src/test/resources/samples/measurements-\{name}.txt");
    Path expectedFile = Paths.get(STR."src/test/resources/samples/measurements-\{name}.out");

    ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    long start = System.currentTimeMillis();
    CalculateAverage_alban_auzeill.execute(inputFile, outputStreamCaptor);
    long end = System.currentTimeMillis();
    System.out.println(STR."Time: \{end - start}ms");
    String expected = Files.readString(expectedFile, UTF_8);
    String actual = outputStreamCaptor.toString();
    assertEquals(expected, actual);
  }

  @Test
  void one_billion() throws IOException, InterruptedException {
    Path inputFile = Paths.get("measurements.txt");
    Path expectedFile = Paths.get("measurements.out");

    ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    long start = System.currentTimeMillis();
    CalculateAverage_alban_auzeill.execute(inputFile, outputStreamCaptor);
    long end = System.currentTimeMillis();
    System.out.println(STR."Time: \{end - start}ms");
    String expected = Files.readString(expectedFile, UTF_8);
    String actual = outputStreamCaptor.toString();
    assertEquals(expected, actual);
  }
}

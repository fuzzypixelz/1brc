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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.zip.CRC32C;

public class CalculateAverage_fuzzypixelz {
    public static void main(String[] argv) throws IOException, InterruptedException {
        Arena arena = Arena.ofShared();
        Worker[] workers = new Worker[PHYSICAL_CORES];

        for (int i = 0; i < PHYSICAL_CORES; i++) {
            workers[i] = new Worker(arena, CHUNK_SIZE * i);
            THREAD_POOL.submit(workers[i]);
        }

        THREAD_LATCH.await();

        for (int i = 0; i < PHYSICAL_CORES; i++) {
            System.err.println(STR."@prefix-\{i} = `\{StandardCharsets.UTF_8.decode(workers[i].prefix.asByteBuffer())}`");
            System.err.println(STR."@suffix-\{i} = `\{StandardCharsets.UTF_8.decode(workers[i].suffix.asByteBuffer())}`");
        }

        THREAD_POOL.shutdown();
    }

    private static class Worker implements Runnable {
        final StationMap map = new StationMap();

        MemorySegment segment;
        MemorySegment prefix;
        MemorySegment suffix;

        final long offset;

        public Worker(Arena arena, long offset) throws IOException {
            this.offset = offset;
            segment = FILE.map(FileChannel.MapMode.READ_ONLY, offset, CHUNK_SIZE, arena);
            long linefeed = 0;
            byte bite = segment.get(ValueLayout.JAVA_BYTE, linefeed);
            while (bite != LF) {
                if (this.offset / CHUNK_SIZE == 6) {
                    System.err.println("@me!");
                }
                linefeed++;
                bite = segment.get(ValueLayout.JAVA_BYTE, linefeed);
            }
            prefix = segment.asSlice(0, linefeed);
            segment = segment.asSlice(linefeed + 1);
        }

        @Override
        public void run() {
            while (segment.byteSize() >= BYTE_VECTOR_SIZE) {
                long offset = advanceFast(segment, map);
                if (offset == 0) {
                    offset = advance2(segment, map);
                    if (offset == 0)
                        break;
                }
                segment = segment.asSlice(offset);

                if (this.offset / CHUNK_SIZE == 6 && segment.byteSize() == 17) {
                    System.err.println(STR."@me! `\{StandardCharsets.UTF_8.decode(segment.asByteBuffer())}`");
                }
            }

            suffix = segment;
            System.err.println(STR."@done! \{offset / CHUNK_SIZE}");
            THREAD_LATCH.countDown();
        }
    }

    private static long advance(MemorySegment segment, StationMap map) {
        long semicolon = 0;
        byte bite = segment.get(ValueLayout.JAVA_BYTE, semicolon);
        while (bite != SEMICOLON && semicolon < segment.byteSize()) {
            semicolon++;
            bite = segment.get(ValueLayout.JAVA_BYTE, semicolon);
        }
        if (semicolon == segment.byteSize())
            return 0;
        final MemorySegment name = segment.asSlice(0, semicolon);

        long linefeed = semicolon;
        bite = segment.get(ValueLayout.JAVA_BYTE, linefeed);
        while (bite != LF && linefeed < segment.byteSize()) {
            linefeed++;
            bite = segment.get(ValueLayout.JAVA_BYTE, linefeed);
        }
        if (linefeed == segment.byteSize())
            return 0;
        final MemorySegment rest = segment.asSlice(semicolon + 1, linefeed - semicolon - 1);
        final double temp = parseTemp(rest);

        map.putOrUpdate(name, temp);

        return linefeed + 1;
    }

    private static long advance2(MemorySegment segment, StationMap map) {
        long semicolon = -1;
        long linefeed = -1;
        long offset = 0;
        while (offset < segment.byteSize()) {
            byte bite = segment.get(ValueLayout.JAVA_BYTE, offset);
            if (bite == SEMICOLON) {
                semicolon = offset;
            }
            if (bite == LF) {
                linefeed = offset;
                break;
            }
            offset++;
        }
        if (linefeed == -1) {
            return 0;
        }
        final MemorySegment name = segment.asSlice(0, semicolon);
        final MemorySegment rest = segment.asSlice(semicolon + 1, linefeed - semicolon - 1);
        final double temp = parseTemp(rest);

        map.putOrUpdate(name, temp);

        return linefeed + 1;
    }

    private static long advanceFast(MemorySegment segment, StationMap map) {
        ByteVector vector = ByteVector
                .fromMemorySegment(BYTE_VECTOR_SPECIES, segment, 0, ByteOrder.LITTLE_ENDIAN);
        final long linefeed = vector.compare(VectorOperators.EQ, LF).firstTrue();
        if (linefeed == BYTE_VECTOR_SPECIES.length())
            return 0; // The line is too big to fit in 128-bits T_T
        final long semicolon = vector.compare(VectorOperators.EQ, SEMICOLON).firstTrue();

        final MemorySegment name = segment.asSlice(0, semicolon);
        final double temp = parseTemp(segment.asSlice(semicolon + 1, linefeed - semicolon - 1));

        map.putOrUpdate(name, temp);

        return linefeed + 1;
    }

    private static double parseTemp(MemorySegment segment) {
        final long length = segment.byteSize();
        final byte x = segment.get(ValueLayout.JAVA_BYTE, length - 1);
        final byte y = segment.get(ValueLayout.JAVA_BYTE, length - 3);
        double temp = (y - ZERO) + (double) (x - ZERO) / 10;
        // NOTE: Four different temperature numbers are possible in the interval [-99.9, 99.9]
        // as long as the fractional digit is always present:
        // Y.X (3 bytes)
        // -Y.X (4 bytes)
        // ZY.X (4 bytes)
        // -ZY.X (5 bytes)
        // Nothing to be done when length == 3 ^_^
        if (length == 4) {
            final byte z = segment.get(ValueLayout.JAVA_BYTE, 0); // length - 4 == 0
            if (z == '-') {
                temp *= -1;
            }
            else {
                temp += (z - ZERO) * 10;
            }
        }
        else if (segment.byteSize() == 5) {
            final byte z = segment.get(ValueLayout.JAVA_BYTE, length - 4);
            temp += (z - ZERO) * 10;
            temp *= -1;
        }
        return temp;
    }

    private record StationEntry(MemorySegment name, double min, double max, double sum, long count) {
    }

    private static class StationMap {
        private static final int MAP_CAPACITY = 10_000 * 3;
        private static final StationEntry[] table = new StationEntry[MAP_CAPACITY];

        public StationMap() {
        }

        public void putOrUpdate(MemorySegment name, double temp) {
            // System.err.println(STR."@name = \{StandardCharsets.UTF_8.decode(name.asByteBuffer())}");
            // System.err.println(STR."@temp = \{temp}");

            int index = Math.floorMod((int) checksum(name), table.length - 1);
            StationEntry entry = table[index];
            while (entry != null
                    && entry.name.byteSize() != name.byteSize()
                    && !entry.name.asByteBuffer().equals(name.asByteBuffer())) {
                index = (index + 1) % (table.length - 1);
                entry = table[index];
            }

            if (entry == null) {
                // Empty slot :)
                table[index] = new StationEntry(name, temp, temp, temp, 1);
            }
            else {
                table[index] = new StationEntry(
                        name,
                        Math.min(entry.min, temp),
                        Math.max(entry.max, temp),
                        entry.sum + temp,
                        entry.count + 1);
            }
        }

        public void debug() {
            for (StationEntry entry : table) {
                if (entry != null) {
                    String name = StandardCharsets.UTF_8.decode(entry.name.asByteBuffer()).toString();
                    String msg = STR."@entry:\{name} -> (min = \{entry.min}, sum = \{round(entry.sum)}, count = \{entry.count}, max = \{entry.max})";
                    System.err.println(msg);
                }
            }
        }

        public String answer() {
            final TreeMap<String, StationEntry> map = new TreeMap<>();
            for (StationEntry entry : table) {
                if (entry != null) {
                    String name = StandardCharsets.UTF_8.decode(entry.name.asByteBuffer()).toString();
                    map.put(name, entry);
                }
            }

            final StringBuilder builder = new StringBuilder();
            builder.append("{");
            for (Map.Entry<String, StationEntry> entry : map.entrySet()) {
                String name = entry.getKey();
                StationEntry s = entry.getValue();
                builder.append(STR."\{name}=\{s.min}/\{round(s.sum / s.count)}/\{s.max}, ");
            }
            builder.delete(builder.length() - 2, builder.length()); // Remove ", " :/
            builder.append("}");

            return builder.toString();
        }
    }

    private static long checksum(MemorySegment name) {
        // CHECKSUM.update(name.asByteBuffer());
        // long value = CHECKSUM.getValue();
        // CHECKSUM.reset();
        // return value;
        return name.asByteBuffer().hashCode();
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    // NOTE: I don't use `Runtime.getRuntime().availableProcessors()` because with Intel Hyper-Threading it would
    // return twice the number of physical cores. As the benchmarks are executed on 8 dedicated AMD vCPUS,
    // and Hetzner states that 1 vCPU = 1 hyper-thread (https://docs.hetzner.com/cloud/servers/faq/),
    // I'm concluding that we have 4 physical cores at our disposal.
    private static final int PHYSICAL_CORES = Runtime.getRuntime().availableProcessors();

    private static final VectorSpecies<Byte> BYTE_VECTOR_SPECIES = ByteVector.SPECIES_128;
    private static final int BYTE_VECTOR_SIZE = BYTE_VECTOR_SPECIES.vectorByteSize();

    private static final String MEASUREMENTS_PATH = "./measurements.txt";
    private static final FileChannel FILE;
    private static final long FILE_SIZE;

    static {
        try {
            FILE = FileChannel.open(Paths.get(MEASUREMENTS_PATH));
            FILE_SIZE = FILE.size();
        }
        catch (IOException exception) {
            throw new RuntimeException("No measurements? Seriously? o_o", exception);
        }
    }

    private static final long CHUNK_SIZE = FILE_SIZE / PHYSICAL_CORES;

    private static final byte ZERO = (byte) '0';
    private static final byte LF = (byte) '\n';
    private static final byte SEMICOLON = (byte) ';';

    // private static final CRC32C CHECKSUM = new CRC32C();
    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(PHYSICAL_CORES);
    private static final CountDownLatch THREAD_LATCH = new CountDownLatch(PHYSICAL_CORES);
}
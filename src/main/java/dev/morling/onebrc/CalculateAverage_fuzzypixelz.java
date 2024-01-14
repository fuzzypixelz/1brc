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
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.zip.CRC32C;

public class CalculateAverage_fuzzypixelz {
    public static void main() throws InterruptedException {
        Thread[] threads = new Thread[AVAILABLE_PROCESSORS];
        Stations[] stationss = new Stations[AVAILABLE_PROCESSORS];

        for (int i = 0; i < AVAILABLE_PROCESSORS; i++) {
            final int threadNumber = i;
            long chunkOffset = i * CHUNK_SIZE;
            long chunkLimit = Math.min((i + 1) * CHUNK_SIZE, FILE_SIZE);
            Thread thread = new Thread(() -> stationss[threadNumber] = processChunk(chunkOffset, chunkLimit));
            threads[i] = thread;
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }


        final TreeMap<String, Aggregator> orderedStations = new TreeMap<>();
        for (Stations stations : stationss) {
            for (int i = 0; i < stations.length(); i++) {
                Aggregator aggregator = stations.aggregatorAt(i);
                if (aggregator != null) {
                    String name = StandardCharsets.UTF_8.decode(aggregator.name.asByteBuffer()).toString();
                    orderedStations.merge(name, aggregator, (a1, a2) -> new Aggregator(
                            MemorySegment.NULL,
                            Math.min(a1.min, a2.min),
                            Math.max(a1.max, a2.max),
                            a1.sum + a2.sum,
                            a1.count + a2.count));
                }
            }
        }

        System.out.println(orderedStations);
    }

    private static Stations processChunk(long chunkOffset, final long chunkLimit) {
        final long indicesSize = (chunkLimit - chunkOffset) / MIN_STRUCTURAL_PER_BYTE + BYTE_VECTOR_SIZE * ValueLayout.JAVA_LONG.byteSize();
        final MemorySegment indices = ARENA.allocateArray(ValueLayout.JAVA_LONG, indicesSize);

        long indicesOffset = 0;

        if (chunkOffset != 0) {
            chunkOffset--;
            while (true) {
                final byte value = CHUNKS.get(ValueLayout.JAVA_BYTE, chunkOffset);
                if (value == LINEFEED) {
                    chunkOffset++;
                    break;
                }
                chunkOffset--;
            }
        }

        long linestart = chunkOffset;

        while (chunkOffset + BYTE_VECTOR_SIZE < chunkLimit) {
            final ByteVector vector = ByteVector.fromMemorySegment(BYTE_VECTOR_SPECIES, CHUNKS, chunkOffset, ByteOrder.LITTLE_ENDIAN);

            long lookup = vector.compare(VectorOperators.EQ, LINEFEED).toLong()
                    | vector.compare(VectorOperators.EQ, SEMICOLON).toLong();

            final long bitCount = Long.bitCount(lookup);
            long offset = indicesOffset;
            while (lookup != 0) {
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset, chunkOffset + Long.numberOfTrailingZeros(lookup));
                offset++;
                lookup &= lookup - 1;
            }

            indicesOffset += bitCount;
            chunkOffset += BYTE_VECTOR_SIZE;
        }

        while (chunkOffset < chunkLimit) {
            final byte value = CHUNKS.get(ValueLayout.JAVA_BYTE, chunkOffset);
            if (value == SEMICOLON || value == LINEFEED) {
                indices.setAtIndex(ValueLayout.JAVA_LONG, indicesOffset, chunkOffset);
                indicesOffset++;
            }
            chunkOffset++;
        }

        if (indicesOffset % 2 == 1) {
            indicesOffset--;
        }

        final Stations stations = new Stations();

        for (int offset = 0; offset < indicesOffset; offset += 2) {
            final long semicolon = indices.getAtIndex(ValueLayout.JAVA_LONG, offset);
            final long linefeed = indices.getAtIndex(ValueLayout.JAVA_LONG, offset + 1);

            final MemorySegment name = CHUNKS.asSlice(linestart, semicolon - linestart);
            // NOTE: Four different temperature numbers are possible in the interval [-99.9, 99.9]
            // as long as the fractional digit is always present:
            // ` Y.X` (3 bytes)
            // ` -Y.X` (4 bytes)
            // ` ZY.X` (4 bytes)
            // `-ZY.X` (5 bytes)
            final long rawTempLength = linefeed - semicolon - 1;
            final byte x = CHUNKS.get(ValueLayout.JAVA_BYTE, linefeed - 1);
            final byte y = CHUNKS.get(ValueLayout.JAVA_BYTE, linefeed - 3);
            final byte z = CHUNKS.get(ValueLayout.JAVA_BYTE, linefeed - 4);
            double temp = ((double) (y - ZERO)) + ((double) (x - ZERO)) / 10;
            // Nothing left to be done if length == 3 ^_^
            final boolean rawTempSizeEq4 = rawTempLength == 4;
            final boolean rawTempSizeEq5 = rawTempLength == 5;
            final boolean zEqDash = z == DASH;
            // To atone for your lack of boolean to int casts, your shall optimize this code Java!
            temp += (((double) (z - ZERO) * 10)) * ((rawTempSizeEq4 && !zEqDash) || rawTempSizeEq5 ? 1 : 0);
            temp *= -1 * ((rawTempSizeEq4 && zEqDash) || rawTempSizeEq5 ? 1 : -1);
            // Aaaaaaaaaaaand all branches break loose...
            stations.putOrUpdate(name, temp);

            linestart = linefeed + 1;
        }

        return stations;
    }

    private record Aggregator(MemorySegment name, double min, double max, double sum, long count) {
        public String toString() {
            return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class Stations {
        private static final long CAPACITY = 10_000 * 3;
        private static final MemoryLayout ENTRY_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_DOUBLE,
                ValueLayout.JAVA_DOUBLE,
                ValueLayout.JAVA_DOUBLE,
                ValueLayout.JAVA_LONG);

        private static final long NAME_ADDRESS_OFFSET = 0;
        private static final long NAME_BYTE_SIZE_OFFSET = NAME_ADDRESS_OFFSET + ValueLayout.JAVA_LONG.byteSize();
        private static final long MIN_OFFSET = NAME_BYTE_SIZE_OFFSET + ValueLayout.JAVA_LONG.byteSize();
        private static final long MAX_OFFSET = MIN_OFFSET + ValueLayout.JAVA_DOUBLE.byteSize();
        private static final long SUM_OFFSET = MAX_OFFSET + ValueLayout.JAVA_DOUBLE.byteSize();
        private static final long COUNT_OFFSET = MAX_OFFSET + ValueLayout.JAVA_DOUBLE.byteSize();

        private static final long ENTRY_SIZE = ENTRY_LAYOUT.byteSize();

        private static CRC32C crc32c;
        private final MemorySegment table;

        public Stations() {
            table = ARENA.allocateArray(ENTRY_LAYOUT, CAPACITY);
            crc32c = new CRC32C();
        }

        public void putOrUpdate(MemorySegment name, double temp) {
            long index = checksum(name) % CAPACITY;
            MemorySegment entryName = getName(index);

            while (entryName.address() != 0
                    && entryName.byteSize() != name.byteSize()
                    && !entryName.asByteBuffer().equals(name.asByteBuffer())) {
                index = (index + 1) % CAPACITY;
                entryName = getName(index);
            }

            if (entryName.address() == 0) {
                put(index, name, temp, temp, temp, 1);
            } else {
                update(index, entryName, temp);
            }
        }

        private void put(long index, MemorySegment name, double min, double max, double sum, long count) {
            table.set(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + NAME_ADDRESS_OFFSET, name.address());
            table.set(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + NAME_BYTE_SIZE_OFFSET, name.byteSize());
            table.set(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + MIN_OFFSET, min);
            table.set(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + MAX_OFFSET, max);
            table.set(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + SUM_OFFSET, sum);
            table.set(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + COUNT_OFFSET, count);
        }

        private MemorySegment getName(long index) {
            long address = table.get(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + NAME_ADDRESS_OFFSET);
            long byteSize = table.get(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + NAME_BYTE_SIZE_OFFSET);
            return MemorySegment.ofAddress(address).reinterpret(byteSize);
        }

        private void update(long index, MemorySegment name, double temp) {
            double min = table.get(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + MIN_OFFSET);
            double max = table.get(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + MAX_OFFSET);
            double sum = table.get(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + SUM_OFFSET);
            long count = table.get(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + COUNT_OFFSET);
            put(index, name, Math.min(min, temp), Math.max(max, temp), sum + temp, count + 1);
        }

        private static long checksum(MemorySegment name) {
            crc32c.update(name.asByteBuffer());
            long value = crc32c.getValue();
            crc32c.reset();
            return value;
        }

        public Aggregator aggregatorAt(long index) {
            MemorySegment name = getName(index);
            if (name.address() == 0) {
                return null;
            } else {
                double min = table.get(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + MIN_OFFSET);
                double max = table.get(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + MAX_OFFSET);
                double sum = table.get(ValueLayout.JAVA_DOUBLE, index * ENTRY_SIZE + SUM_OFFSET);
                long count = table.get(ValueLayout.JAVA_LONG, index * ENTRY_SIZE + COUNT_OFFSET);
                return new Aggregator(name, min, max, sum, count);
            }
        }

        public long length() {
            return CAPACITY;
        }
    }

    private static final VectorSpecies<Byte> BYTE_VECTOR_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int BYTE_VECTOR_SIZE = BYTE_VECTOR_SPECIES.vectorByteSize();

    private static final Arena ARENA = Arena.global();

    private static final String MEASUREMENTS_PATH = "./measurements.txt";
    private static final FileChannel FILE;
    private static final long FILE_SIZE;
    private static final MemorySegment CHUNKS;

    static {
        try {
            FILE = FileChannel.open(Paths.get(MEASUREMENTS_PATH));
            FILE_SIZE = FILE.size();
            CHUNKS = FILE.map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE, ARENA);
        } catch (IOException exception) {
            throw new RuntimeException("No measurements? >:(", exception);
        }
    }

    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final long CHUNK_SIZE = Math.ceilDiv(CHUNKS.byteSize(), AVAILABLE_PROCESSORS);

    private static final byte LINEFEED = (byte) '\n';
    private static final byte SEMICOLON = (byte) ';';
    private static final byte DASH = (byte) '-';
    private static final byte ZERO = (byte) '0';

    private static final long MIN_STRUCTURAL_PER_BYTE = 6;
}

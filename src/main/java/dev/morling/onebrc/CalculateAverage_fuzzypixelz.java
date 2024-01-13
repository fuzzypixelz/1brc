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
import java.nio.file.Paths;

public class CalculateAverage_fuzzypixelz {
    public static void main() throws InterruptedException {
        Thread[] threads = new Thread[AVAILABLE_PROCESSORS];

        System.err.println(STR."@file-size = \{FILE_SIZE}");

        for (int i = 0; i < AVAILABLE_PROCESSORS; i++) {
            long chunkOffset = i * CHUNK_SIZE;
            long chunkLimit = Math.min((i + 1) * CHUNK_SIZE, FILE_SIZE);
            System.err.println(STR."@chunk-offset = \{chunkOffset}");
            System.err.println(STR."@chunk-limit = \{chunkLimit}");
            final int threadNumber = i;
            Thread thread = new Thread(() -> processChunk(chunkOffset, chunkLimit, threadNumber));
            threads[i] = thread;
            thread.start();
        }
        
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static void processChunk(long chunkOffset, final long chunkLimit, final int threadNumber) {
        final int threadDebugged = 1;
        final MemorySegment indices = ARENA.allocateArray(ValueLayout.JAVA_LONG, ((chunkLimit - chunkOffset) / 6));

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
            final ByteVector vector =
                    ByteVector.fromMemorySegment(BYTE_VECTOR_SPECIES, CHUNKS, chunkOffset, ByteOrder.LITTLE_ENDIAN);

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

        for (int offset = 0; offset < indicesOffset; offset += 2) {
            final long semicolon = indices.getAtIndex(ValueLayout.JAVA_LONG, offset);
            final long linefeed = indices.getAtIndex(ValueLayout.JAVA_LONG, offset + 1);

            final MemorySegment rawName = CHUNKS.asSlice(linestart, semicolon - linestart);

            // NOTE: Four different temperature numbers are possible in the interval [-99.9, 99.9]
            // as long as the fractional digit is always present:
            // `  Y.X` (3 bytes)
            // ` -Y.X` (4 bytes)
            // ` ZY.X` (4 bytes)
            // `-ZY.X` (5 bytes)
            final long rawTempLength = linefeed - semicolon - 1;
            final byte x = CHUNKS.get(ValueLayout.JAVA_BYTE, linefeed - 1);
            final byte y = CHUNKS.get(ValueLayout.JAVA_BYTE, linefeed - 3);
            final byte z = CHUNKS.get(ValueLayout.JAVA_BYTE, linefeed - 4);
            int temp = (y - ZERO) * 10 + (x - ZERO);
            // Nothing left to be done if length == 3 ^_^
            final boolean rawTempSizeEq4 = rawTempLength == 4;
            final boolean rawTempSizeEq5 = rawTempLength == 5;
            final boolean zEqDash = z == DASH;
            // To atone for your lack of boolean to int casts, your shall optimize this code Java!
            temp += ((z - ZERO) * 100) * ((rawTempSizeEq4 && !zEqDash) || rawTempSizeEq5 ? 1 : 0);
            temp *= -1 * ((rawTempSizeEq4 && zEqDash) || rawTempSizeEq5 ? 1 : -1);

            if (threadNumber == threadDebugged) {
                System.err.println(STR."@thread-\{threadNumber}: `\{new String(rawName.toArray(ValueLayout.JAVA_BYTE))};\{((double) temp) / 10}`");
            }

            linestart = linefeed + 1;
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
        }
        catch (IOException exception) {
            throw new RuntimeException("No measurements? >:(", exception);
        }
    }
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final long CHUNK_SIZE = Math.ceilDiv(CHUNKS.byteSize(), AVAILABLE_PROCESSORS);

    private static final byte LINEFEED = (byte) '\n';
    private static final byte SEMICOLON = (byte) ';';
    private static final byte DASH = (byte) '-';
    private static final byte ZERO = (byte) '0';
}
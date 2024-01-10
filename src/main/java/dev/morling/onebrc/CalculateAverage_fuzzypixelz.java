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
    public static void main(String[] argv) throws IOException, InterruptedException {
        MemorySegment indices = ARENA.allocateArray(ValueLayout.JAVA_LONG, (FILE_SIZE / 6) + 8);
        long dataOffset = 0;
        long indicesOffset = 0;

        while (dataOffset < DATA.byteSize() - BYTE_VECTOR_SIZE) {
            ByteVector vector =
                    ByteVector.fromMemorySegment(BYTE_VECTOR_SPECIES, DATA, dataOffset, ByteOrder.LITTLE_ENDIAN);

            long lookup = vector.compare(VectorOperators.EQ, LF).toLong()
                    | vector.compare(VectorOperators.EQ, SEMICOLON).toLong();

            long bitCount = Long.bitCount(lookup);
            long offset = indicesOffset;
            while (lookup != 0) {
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
                indices.setAtIndex(ValueLayout.JAVA_LONG, offset++, dataOffset + Long.numberOfTrailingZeros(lookup));
                lookup &= lookup - 1;
            }

            indicesOffset += bitCount;
            dataOffset += BYTE_VECTOR_SIZE;
        }
    }

    private static final VectorSpecies<Byte> BYTE_VECTOR_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int BYTE_VECTOR_SIZE = BYTE_VECTOR_SPECIES.vectorByteSize();

    private static final Arena ARENA = Arena.global();

    private static final String MEASUREMENTS_PATH = "./measurements.txt";
    private static final FileChannel FILE;
    private static final long FILE_SIZE;
    private static final MemorySegment DATA;
    static {
        try {
            FILE = FileChannel.open(Paths.get(MEASUREMENTS_PATH));
            FILE_SIZE = FILE.size();
            DATA = FILE.map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE, ARENA);
        }
        catch (IOException exception) {
            throw new RuntimeException("No measurements? >:(", exception);
        }
    }

    private static final byte LF = (byte) '\n';
    private static final byte SEMICOLON = (byte) ';';
}
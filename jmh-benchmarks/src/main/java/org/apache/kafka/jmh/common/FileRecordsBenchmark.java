/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.cache;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.utils.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a simple example of a JMH benchmark.
 *
 * The sample code provided by the JMH project is a great place to start learning how to write correct benchmarks:
 * http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FileRecordsBenchmark {
    private static final File fileBefore = new File("./testFileBefore.txt");
    private static final File fileAfter = new File("./testFileAfter.txt");
    private static final boolean mutable = true;
    private static final boolean fileAlreadyExists = false;
    private static final int initFileSize = 512 * 1025 *1024;
    private static final boolean preallocate = true;

	/**
     * Open a channel for the given file
     * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
     * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre-allocate file or not, gotten from configuration.
     */
    private FileChannel openChannelBefore(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists || !preallocate) {
                return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(initFileSize);
                return randomAccessFile.getChannel();
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }
	
	/**
     * Open a channel for the given file
     * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
     * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre-allocate file or not, gotten from configuration.
     */
    private FileChannel openChannelAfter(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (preallocate && !fileAlreadyExists) {
                return Utils.createPreallocatedFile(file.toPath(), initFileSize);
            } else {
                return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }
	
    @Benchmark
	public FileChannel testOpenChannelBefore() throws IOException {
		Files.deleteIfExists(fileBefore.toPath());
        FileChannel fileChannel =  openChannelBefore(fileBefore, mutable, fileAlreadyExists, initFileSize, preallocate);
		fileChannel.close();
		
		return fileChannel;
	}
	
    @Benchmark
	public FileChannel testOpenChannelAfter() throws IOException {
		Files.deleteIfExists(fileAfter.toPath());
        FileChannel fileChannel = openChannelAfter(fileAfter, mutable, fileAlreadyExists, initFileSize, preallocate);
		fileChannel.close();
		
		return fileChannel;
	}

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FileRecordsBenchmark.class.getSimpleName())
                .forks(2)
                .build();

        new Runner(opt).run();
    }

}

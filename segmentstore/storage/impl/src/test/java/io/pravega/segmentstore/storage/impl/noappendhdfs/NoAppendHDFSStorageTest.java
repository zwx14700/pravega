/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.noappendhdfs;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.NoAppendSyncStorage;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.impl.hdfs.HDFSClusterHelpers;
import io.pravega.segmentstore.storage.impl.hdfs.HDFSStorageConfig;
import io.pravega.segmentstore.storage.rolling.NoAppendRollingStorage;
import io.pravega.segmentstore.storage.rolling.RollingSegmentHandle;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Unit tests for HDFSStorage.
 */
public class NoAppendHDFSStorageTest {
    //region RollingStorageTests

    /**
     * Tests the HDFSStorage adapter with a RollingStorage wrapper.
     */
    public static class RollingStorageTests extends RollingStorageTestBase {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private File baseDir = null;
        private MiniDFSCluster hdfsCluster = null;
        private HDFSStorageConfig adapterConfig;

        @Before
        public void setUp() throws Exception {
            this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());
            this.adapterConfig = HDFSStorageConfig
                    .builder()
                    .with(HDFSStorageConfig.REPLICATION, 1)
                    .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                    .build();
        }

        @After
        public void tearDown() {
            if (hdfsCluster != null) {
                hdfsCluster.shutdown();
                hdfsCluster = null;
                FileHelpers.deleteFileOrDirectory(baseDir);
                baseDir = null;
            }
        }

        @Override
        protected Storage createStorage() {
            return wrap(new TestHDFSStorage(this.adapterConfig));
        }

        @Override
        protected Storage wrap(SyncStorage storage) {
            return new AsyncStorageWrapper(new NoAppendRollingStorage((NoAppendSyncStorage) storage, SegmentRollingPolicy.ALWAYS_ROLLING), executorService());
        }

        /**
         * Tests a scenario that would concatenate various segments successively into an initially empty segment while not
         * producing an excessive number of chunks. The initial concat will use header merge since the segment has no chunks,
         * but successive concats should unseal that last chunk and concat to it using the native method.
         * <p>
         * NOTE: this could be moved down into RollingStorageTests.java, however it being here ensures that unseal() is being
         * exercised in all classes that derive from this, which is all of the Storage implementations.
         *
         * @throws Exception If one occurred.
         */
        @Override
        @Test
        public void testSuccessiveConcats() throws Exception {
            final String segmentName = "Segment";
            final int writeLength = 21;
            final int concatCount = 10;

            @Cleanup
            val s = createStorage();
            s.initialize(1);

            // Create Target Segment with infinite rolling. Do not write anything to it yet.
            val writeHandle = s.create(segmentName, SegmentRollingPolicy.NO_ROLLING, TIMEOUT)
                               .thenCompose(v -> s.openWrite(segmentName)).join();

            final Random rnd = new Random(0);
            byte[] writeBuffer = new byte[writeLength];
            val writeStream = new ByteArrayOutputStream();
            for (int i = 0; i < concatCount; i++) {
                // Create a source segment, write a little bit to it, then seal & merge it.
                String sourceSegment = segmentName + "_Source_" + i;
                val sourceHandle = s.create(sourceSegment, TIMEOUT).thenCompose(v -> s.openWrite(sourceSegment)).join();
                rnd.nextBytes(writeBuffer);
                s.write(sourceHandle, 0, new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
                s.seal(sourceHandle, TIMEOUT).join();
                s.concat(writeHandle, writeStream.size(), sourceSegment, TIMEOUT).join();
                writeStream.write(writeBuffer);
            }

            // Write directly to the target segment - this ensures that writes themselves won't create a new chunk if the
            // write can still fit into the last chunk.
            rnd.nextBytes(writeBuffer);
            s.write(writeHandle, writeStream.size(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
            writeStream.write(writeBuffer);

            // Get a read handle, which will also fetch the number of chunks for us.
            val readHandle = (RollingSegmentHandle) s.openRead(segmentName).join();
            Assert.assertEquals("Unexpected number of chunks created.", 32, readHandle.chunks().size());
            val writtenData = writeStream.toByteArray();
            byte[] readBuffer = new byte[writtenData.length];
            int bytesRead = s.read(readHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
            Assert.assertArrayEquals("Unexpected data read back.", writtenData, readBuffer);
        }

        @Override
        @Test
        public void testWriteAfterHeaderMerge() throws Exception {
            final String segmentName = "Segment";
            final int writeLength = 21;

            @Cleanup
            val s = createStorage();
            s.initialize(1);

            // Create Target Segment with infinite rolling. Do not write anything to it yet.
            val writeHandle = s.create(segmentName, SegmentRollingPolicy.NO_ROLLING, TIMEOUT)
                               .thenCompose(v -> s.openWrite(segmentName)).join();

            final Random rnd = new Random(0);
            byte[] writeBuffer = new byte[writeLength];
            val writeStream = new ByteArrayOutputStream();

            // Create a source segment, write a little bit to it, then seal & merge it.
            String sourceSegment = segmentName + "_Source";
            val sourceHandle = s.create(sourceSegment, TIMEOUT).thenCompose(v -> s.openWrite(sourceSegment)).join();
            rnd.nextBytes(writeBuffer);
            s.write(sourceHandle, 0, new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
            s.seal(sourceHandle, TIMEOUT).join();
            s.concat(writeHandle, writeStream.size(), sourceSegment, TIMEOUT).join();
            writeStream.write(writeBuffer);

            // Write directly to the target segment.
            rnd.nextBytes(writeBuffer);
            s.write(writeHandle, writeStream.size(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
            writeStream.write(writeBuffer);

            // Get a read handle, which will also fetch the number of chunks for us.
            val readHandle = (RollingSegmentHandle) s.openRead(segmentName).join();
            Assert.assertEquals("Unexpected number of chunks created.", 5, readHandle.chunks().size());
        }

        /**
         * Tests the concat() method.
         *
         * @throws Exception if an unexpected error occurred.
         */
        @Override
        @Test
        public void testConcat() throws Exception {
            final String context = "Concat";
            try (Storage s = createStorage()) {
                s.initialize(DEFAULT_EPOCH);
                HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

                // Check invalid segment name.
                String firstSegmentName = getSegmentName(0, context);
                SegmentHandle firstSegmentHandle = s.openWrite(firstSegmentName).join();
                val sealedSegmentName = "SealedSegment";
                createSegment(sealedSegmentName, s);
                val sealedSegmentHandle = s.openWrite(sealedSegmentName).join();
                s.write(sealedSegmentHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
                s.seal(sealedSegmentHandle, TIMEOUT).join();
                AtomicLong firstSegmentLength = new AtomicLong(s.getStreamSegmentInfo(firstSegmentName,
                        TIMEOUT).join().getLength());
                assertThrows("concat() did not throw for non-existent target segment name.",
                        () -> s.concat(createInexistentSegmentHandle(s, false), 0, sealedSegmentName, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);

                assertThrows("concat() did not throw for invalid source StreamSegment name.",
                        () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), "foo2", TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);

                ArrayList<String> concatOrder = new ArrayList<>();
                concatOrder.add(firstSegmentName);
                for (String sourceSegment : appendData.keySet()) {
                    if (sourceSegment.equals(firstSegmentName)) {
                        // FirstSegment is where we'll be concatenating to.
                        continue;
                    }

                    assertThrows("Concat allowed when source segment is not sealed.",
                            () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceSegment, TIMEOUT),
                            ex -> ex instanceof IllegalStateException);

                    // Seal the source segment and then re-try the concat
                    val sourceWriteHandle = s.openWrite(sourceSegment).join();
                    s.seal(sourceWriteHandle, TIMEOUT).join();
                    SegmentProperties preConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                    SegmentProperties sourceProps = s.getStreamSegmentInfo(sourceSegment, TIMEOUT).join();

                    s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceSegment, TIMEOUT).join();
                    concatOrder.add(sourceSegment);
                    SegmentProperties postConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                    // This concat does not delete the original segment
                    // Assert.assertFalse("concat() did not delete source segment", s.exists(sourceSegment, TIMEOUT).join());

                    // Only check lengths here; we'll check the contents at the end.
                    Assert.assertEquals("Unexpected target StreamSegment.length after concatenation.",
                            preConcatTargetProps.getLength() + sourceProps.getLength(), postConcatTargetProps.getLength());
                    firstSegmentLength.set(postConcatTargetProps.getLength());
                }

                // Check the contents of the first StreamSegment. We already validated that the length is correct.
                SegmentProperties segmentProperties = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                byte[] readBuffer = new byte[(int) segmentProperties.getLength()];

                // Read the entire StreamSegment.
                int bytesRead = s.read(firstSegmentHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
                Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);

                // Check, concat-by-concat, that the final data is correct.
                int offset = 0;
                for (String segmentName : concatOrder) {
                    byte[] concatData = appendData.get(segmentName).toByteArray();
                    AssertExtensions.assertArrayEquals("Unexpected concat data.", concatData, 0, readBuffer, offset,
                            concatData.length);
                    offset += concatData.length;
                }

                Assert.assertEquals("Concat included more bytes than expected.", offset, readBuffer.length);
            }
        }

        @Override
        protected long getSegmentRollingSize() {
            // Need to increase this otherwise the test will run for too long.
            return SegmentRollingPolicy.ALWAYS_ROLLING.getMaxLength();
        }
    }

    //endregion

    //region TestHDFSStorage

    /**
     * Special HDFSStorage that uses a modified version of the MiniHDFSCluster DistributedFileSystem which fixes the
     * 'read-only' permission issues observed with that one.
     **/
    private static class TestHDFSStorage extends NoAppendHDFSStorage {
        TestHDFSStorage(HDFSStorageConfig config) {
            super(config);
        }

        @Override
        protected FileSystem openFileSystem(Configuration conf) throws IOException {
            return new FileSystemFixer(conf);
        }
    }

    private static class FileSystemFixer extends DistributedFileSystem {
        @SneakyThrows(IOException.class)
        FileSystemFixer(Configuration conf) {
            initialize(FileSystem.getDefaultUri(conf), conf);
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
            if (getFileStatus(f).getPermission().getUserAction() == FsAction.READ) {
                throw new AclException(f.getName());
            }

            return super.append(f, bufferSize, progress);
        }

        @Override
        public void concat(Path targetPath, Path[] sourcePaths) throws IOException {
            if (getFileStatus(targetPath).getPermission().getUserAction() == FsAction.READ) {
                throw new AclException(targetPath.getName());
            }

            super.concat(targetPath, sourcePaths);
        }
    }

    //endregion
}

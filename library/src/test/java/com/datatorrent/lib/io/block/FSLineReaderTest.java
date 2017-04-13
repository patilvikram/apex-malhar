/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.io.block;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.testbench.CollectorTestSink;

import static com.datatorrent.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class FSLineReaderTest
{
  AbstractFSBlockReader<String> getBlockReader()
  {
    return new BlockReader();
  }

  public class TestMeta extends TestWatcher
  {
    String dataFilePath;
    File dataFile;
    Context.OperatorContext readerContext;
    AbstractFSBlockReader<String> blockReader;
    CollectorTestSink<Object> blockMetadataSink;
    CollectorTestSink<Object> messageSink;

    List<String[]> messages = Lists.newArrayList();
    String appId;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.dataFilePath = "src/test/resources/reader_test_data.csv";
      this.dataFile = new File(dataFilePath);
      appId = Long.toHexString(System.currentTimeMillis());
      blockReader = getBlockReader();

      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(DAG.APPLICATION_ID, appId);
      readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);
      readerContext = mockOperatorContext(1, readerAttr);

      blockReader.setup(readerContext);

      messageSink = new CollectorTestSink<>();
      blockReader.messages.setSink(messageSink);

      blockMetadataSink = new CollectorTestSink<>();
      blockReader.blocksMetadataOutput.setSink(blockMetadataSink);

      BufferedReader reader;
      try {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.dataFile.getAbsolutePath())));
        String line;
        while ((line = reader.readLine()) != null) {
          messages.add(line.split(","));
        }
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      blockReader.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void test()
  {

    BlockMetadata.FileBlockMetadata block = new BlockMetadata.FileBlockMetadata(testMeta.dataFile.getAbsolutePath(), 0L,
        0L, testMeta.dataFile.length(),
        true, -1);

    testMeta.blockReader.beginWindow(1);
    testMeta.blockReader.blocksMetadataInput.process(block);
    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());

    for (int i = 0; i < messages.size(); i++) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<String> msg = (AbstractBlockReader.ReaderRecord<String>)messages.get(i);
      Assert.assertTrue("line " + i, Arrays.equals(msg.getRecord().split(","), testMeta.messages.get(i)));
    }
  }

  @Test
  public void testMultipleBlocks()
  {
    long blockSize = 1000;
    int noOfBlocks = (int)((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ?
        0 :
        1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata = new BlockMetadata.FileBlockMetadata(
          testMeta.dataFile.getAbsolutePath(), i, i * blockSize,
          i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize, i == noOfBlocks - 1, i - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());
    for (int i = 0; i < messages.size(); i++) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<String> msg = (AbstractBlockReader.ReaderRecord<String>)messages.get(i);
      Assert.assertTrue("line " + i, Arrays.equals(msg.getRecord().split(","), testMeta.messages.get(i)));
    }
  }

  @Test
  public void testNonConsecutiveBlocks()
  {
    long blockSize = 1000;
    int noOfBlocks = (int)((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ?
        0 :
        1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < Math.ceil(noOfBlocks / 10.0); j++) {
        int blockNo = 10 * j + i;
        if (blockNo >= noOfBlocks) {
          continue;
        }
        BlockMetadata.FileBlockMetadata blockMetadata = new BlockMetadata.FileBlockMetadata(
            testMeta.dataFile.getAbsolutePath(), blockNo, blockNo * blockSize,
            blockNo == noOfBlocks - 1 ? testMeta.dataFile.length() : (blockNo + 1) * blockSize,
            blockNo == noOfBlocks - 1, blockNo - 1);
        testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
      }
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());

    Collections.sort(testMeta.messages, new Comparator<String[]>()
    {
      @Override
      public int compare(String[] rec1, String[] rec2)
      {
        return compareStringArrayRecords(rec1, rec2);
      }
    });

    Collections.sort(messages, new Comparator<Object>()
    {
      @Override
      public int compare(Object object1, Object object2)
      {
        @SuppressWarnings("unchecked")
        String[] rec1 = ((AbstractBlockReader.ReaderRecord<String>)object1).getRecord().split(",");
        @SuppressWarnings("unchecked")
        String[] rec2 = ((AbstractBlockReader.ReaderRecord<String>)object2).getRecord().split(",");
        return compareStringArrayRecords(rec1, rec2);
      }
    });

    for (int i = 0; i < messages.size(); i++) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<String> msg = (AbstractBlockReader.ReaderRecord<String>)messages.get(i);
      Assert.assertTrue("line " + i, Arrays.equals(msg.getRecord().split(","), testMeta.messages.get(i)));
    }
  }

  public static final class BlockReader extends AbstractFSBlockReader.AbstractFSLineReader<String>
  {
    private final Pattern datePattern = Pattern.compile("\\d{2}?/\\d{2}?/\\d{4}?");

    @Override
    protected String convertToRecord(byte[] bytes)
    {
      String record = new String(bytes);
      String[] parts = record.split(",");
      return parts.length > 0 && datePattern.matcher(parts[0]).find() ? record : null;
    }
  }

  /**
   * Utility function to compare lexicographically 2 records of string arrays
   *
   * @param rec1
   * @param rec2
   * @return negative if rec1 < rec2, positive if rec1 > rec2, 0 otherwise
   */
  private int compareStringArrayRecords(String[] rec1, String[] rec2)
  {
    for (int i = 0; i < rec1.length && i < rec2.length; i++) {
      if (rec1[i].equals(rec2[i])) {
        continue;
      }
      return rec1[i].compareTo(rec2[i]);
    }
    return 0;
  }

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(FSLineReaderTest.class);
}

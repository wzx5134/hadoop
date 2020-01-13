package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TestBlockPlacementPolicyDebugLoggingBuilder extends BaseReplicationPolicyTest {

  public TestBlockPlacementPolicyDebugLoggingBuilder() {
    this.blockPlacementPolicy = BlockPlacementPolicyDefault.class.getName();
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    final String[] racks = {
        "/d1/r1/n1",
        "/d1/r1/n2",
        "/d1/r2/n3",
    };
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    return DFSTestUtil.toDatanodeDescriptor(storages);
  }

  @Test
  public void testChooseRandomDynamicallyChangeLogger() throws Exception {
    BlockPlacementPolicyDefault repl =
        spy((BlockPlacementPolicyDefault) replicator);

    GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG,
        org.slf4j.event.Level.INFO);
    List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>();
    results.add(storages[0]);
    results.add(storages[1]);
    results.add(storages[2]);
    Set<Node> excludeNodes = new HashSet<>();
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG,
            org.slf4j.event.Level.DEBUG);
        return dataNodes[0];
      }
    }).when(repl).chooseDataNode("/",excludeNodes);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG,
            org.slf4j.event.Level.DEBUG);
        return dataNodes[0];
      }
    }).when(repl).chooseDataNode("/",excludeNodes, StorageType.DISK);
    EnumMap<StorageType, Integer> types = new EnumMap<>(StorageType.class);
    types.put(StorageType.DISK, 1);
    repl.chooseRandom(1, "/", excludeNodes, 1024L, 3, results, false, types);
  }
}

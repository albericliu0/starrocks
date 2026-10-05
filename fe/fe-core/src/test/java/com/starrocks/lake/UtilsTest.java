// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.MockedBackend;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class UtilsTest {

    @Mocked
    NodeMgr nodeMgr;

    @Test
    public void testChooseBackend() {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                SystemInfoService systemInfo = new SystemInfoService();
                return systemInfo;
            }
        };

        new MockUp<LakeTablet>() {
            @Mock
            public long getPrimaryComputeNodeId(long clusterId) throws StarRocksException {
                throw new StarRocksException("Failed to get primary backend");
            }
        };

        new MockUp<NodeSelector>() {
            @Mock
            public Long seqChooseBackendOrComputeId() throws StarRocksException {
                throw new StarRocksException("No backend or compute node alive.");
            }
        };
    }

    @Test
    public void testGetWarehouseIdByNodeId() {
        SystemInfoService systemInfo = new SystemInfoService();
        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        b1.setBePort(9060);
        b1.setWarehouseId(10001L);
        Backend b2 = new Backend(10002L, "192.168.0.2", 9050);
        b2.setBePort(9060);
        b2.setWarehouseId(10002L);

        // add two backends to different warehouses
        systemInfo.addBackend(b1);
        systemInfo.addBackend(b2);

        // If the version of be is old, it may pass null.
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                Utils.getWarehouseIdByNodeId(systemInfo, 0).orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID).longValue());

        // pass a wrong tBackend
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                Utils.getWarehouseIdByNodeId(systemInfo, 10003).orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID).longValue());

        // pass a right tBackend
        Assertions.assertEquals(10001L, Utils.getWarehouseIdByNodeId(systemInfo, 10001).get().longValue());
        Assertions.assertEquals(10002L, Utils.getWarehouseIdByNodeId(systemInfo, 10002).get().longValue());
    }

    private MockedBackend.MockLakeService mockTwoNodePublish(ComputeNode node1, ComputeNode node2,
                                                             int failedStatusCode) {
        MockedBackend.MockLakeService lakeService = new MockedBackend.MockLakeService() {
            @Override
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.compactionScores = new HashMap<>();
                if (request.tabletIds.contains(3L)) {
                    // node2 owns tablets 3 and 4; 3 is still being applied from an earlier request.
                    response.failedTablets = Lists.newArrayList(3L);
                    response.status.statusCode = failedStatusCode;
                    response.status.errorMsgs = Lists.newArrayList(
                            "The previous publish version task for tablet 3 has not finished");
                    response.compactionScores.put(4L, 4.0);
                } else {
                    response.status.statusCode = 0;
                    for (Long tabletId : request.tabletIds) {
                        response.compactionScores.put(tabletId, (double) tabletId);
                    }
                }
                return CompletableFuture.completedFuture(response);
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return new WarehouseManager();
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public boolean warehouseExists(long warehouseId) {
                return true;
            }

            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(Long warehouseId, LakeTablet tablet) {
                return tablet.getId() <= 2 ? node1 : node2;
            }
        };
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };
        return lakeService;
    }

    @Test
    public void testPublishVersionBatchKeepsWhatOtherNodesPublished() {
        ComputeNode node1 = new ComputeNode(1001L, "127.0.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1002L, "127.0.0.2", 9040);
        node2.setBrpcPort(9050);
        mockTwoNodePublish(node1, node2, TStatusCode.RESOURCE_BUSY.getValue());

        List<Tablet> tablets = Lists.newArrayList(new LakeTablet(1L), new LakeTablet(2L), new LakeTablet(3L),
                new LakeTablet(4L));
        Map<Long, Double> compactionScores = new HashMap<>();
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        PublishVersionPartialFailureException ex = Assertions.assertThrows(
                PublishVersionPartialFailureException.class,
                () -> Utils.publishVersionBatch(tablets, Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                        compactionScores, nodeToTablets, WarehouseManager.DEFAULT_WAREHOUSE_ID, null));

        // Only the busy tablet is reported, and it is reported as "still in progress".
        Assertions.assertEquals(Lists.newArrayList(3L), Lists.newArrayList(ex.getFailedTabletIds()));
        Assertions.assertTrue(ex.isInProgress());
        Assertions.assertTrue(ex.getMessage().contains("tablets [3]"), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("127.0.0.2"), ex.getMessage());

        // What the nodes did publish is kept for the caller: scores of 1, 2 (node1) and 4 (node2) ...
        Assertions.assertEquals(3, compactionScores.size());
        Assertions.assertEquals(4.0, compactionScores.get(4L));
        Assertions.assertFalse(compactionScores.containsKey(3L));
        // ... and the routing map names only published tablets, so their txn logs can be deleted later.
        Assertions.assertEquals(Lists.newArrayList(1L, 2L), nodeToTablets.get(node1));
        Assertions.assertEquals(Lists.newArrayList(4L), nodeToTablets.get(node2));
    }

    @Test
    public void testPublishVersionBatchRealFailureIsNotInProgress() {
        ComputeNode node1 = new ComputeNode(1003L, "127.0.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1004L, "127.0.0.2", 9040);
        node2.setBrpcPort(9050);
        mockTwoNodePublish(node1, node2, TStatusCode.INTERNAL_ERROR.getValue());

        List<Tablet> tablets = Lists.newArrayList(new LakeTablet(1L), new LakeTablet(3L));
        PublishVersionPartialFailureException ex = Assertions.assertThrows(
                PublishVersionPartialFailureException.class,
                () -> Utils.publishVersionBatch(tablets, Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                        new HashMap<>(), new HashMap<>(), WarehouseManager.DEFAULT_WAREHOUSE_ID, null));
        Assertions.assertEquals(Lists.newArrayList(3L), Lists.newArrayList(ex.getFailedTabletIds()));
        Assertions.assertFalse(ex.isInProgress());
    }

    @Test
    public void testIsPublishInProgressStatus() {
        Assertions.assertFalse(Utils.isPublishInProgressStatus(null));
        Assertions.assertTrue(Utils.isPublishInProgressStatus(TStatusCode.RESOURCE_BUSY.getValue()));
        Assertions.assertTrue(Utils.isPublishInProgressStatus(TStatusCode.TIMEOUT.getValue()));
        Assertions.assertTrue(Utils.isPublishInProgressStatus(TStatusCode.PUBLISH_TIMEOUT.getValue()));
        Assertions.assertFalse(Utils.isPublishInProgressStatus(TStatusCode.INTERNAL_ERROR.getValue()));
        Assertions.assertFalse(Utils.isPublishInProgressStatus(TStatusCode.OK.getValue()));
    }
}

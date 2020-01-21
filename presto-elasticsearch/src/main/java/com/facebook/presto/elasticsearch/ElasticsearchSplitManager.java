/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        ElasticsearchTableLayoutHandle layoutHandle = (ElasticsearchTableLayoutHandle) layout;
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();
        ElasticsearchTableDescription table = client.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        verify(table != null, "Table no longer exists: %s", tableHandle.toString());

        // TODO: 如果生成Split后，SourceTask执行前，ES Index的Shard发生了relocation怎么办？
        List<String> indices = client.getIndices(table);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (String index : indices) {
            ClusterSearchShardsResponse response = client.getSearchShards(index, table);
            DiscoveryNode[] nodes = response.getNodes();
            // Note: DiscoveryNode 是啥 ? Answer: Shard所在的DataNode.

            ImmutableMap.Builder<String, DiscoveryNode> nodeIdMapBuilder = ImmutableMap.builder();
            for (DiscoveryNode discoveryNode : nodes) {
                // TODO: 用哪个ID？
                // nodeIdMap.put(discoveryNode.getEphemeralId(), discoveryNode);
                nodeIdMapBuilder.put(discoveryNode.getId(), discoveryNode);
            }

            ImmutableMap<String, DiscoveryNode> nodeIdMap = nodeIdMapBuilder.build();

            for (ClusterSearchShardsGroup group : response.getGroups()) {

                // Note: 当Presto与ES混部署时，如何实现数据本地性？Answer: 逻辑在这里
                // Note: ClusterSearchShardsGroup 指的shard包含primary + replica ？ 所以是 group ? 从这个类的私有成员ShardRouting列表来看是这样的
                // TODO: 选取哪个shard副本(本质上是选取这个shard所在的Node)的逻辑也可以优化，目前先默认选择了第一个
                //   可供参考的选取方式如下：
                //   （1）Presto Coord 中存储对应Shard的请求次数，据此信息每次来选取请求次数最少的那个Shard
                //   （2）利用ES的ARS来选择，问题是如何调用ARS算法，获取到ARS的结果
                //   （3）随机选择
                int shardCopyCount = group.getShards().length;
                int selectedShardCopyIndex = 0; // TODO: 优化选择方式
                String currentNodeId = group.getShards()[selectedShardCopyIndex].currentNodeId();
                DiscoveryNode currentNode = nodeIdMap.get(currentNodeId);

                ElasticsearchSplit split = new ElasticsearchSplit(
                        index,
                        table.getType(),
                        group.getShardId().getId(),
                        currentNode.getHostName(),
                        currentNode.getAddress().getPort(),
                        layoutHandle.getTupleDomain()); // TODO: layoutHandle 是啥？TupleDomain 是啥？
                splits.add(split);
            }
        }
        return new FixedSplitSource(splits.build());
    }
}

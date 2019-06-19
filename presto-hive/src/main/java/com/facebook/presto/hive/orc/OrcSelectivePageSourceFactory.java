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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveSelectivePageSourceFactory;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilterUtils;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;
import static com.facebook.presto.hive.HiveUtil.typedPartitionKey;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcSelectivePageSourceFactory
        implements HiveSelectivePageSourceFactory
{
    private final TypeManager typeManager;
    private final boolean useOrcColumnNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;

    @Inject
    public OrcSelectivePageSourceFactory(TypeManager typeManager, HiveClientConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this(typeManager, requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames(), hdfsEnvironment, stats, config.getDomainCompactionThreshold());
    }

    public OrcSelectivePageSourceFactory(TypeManager typeManager, boolean useOrcColumnNames, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, int domainCompactionThreshold)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useOrcColumnNames = useOrcColumnNames;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
            List<Integer> outputColumns,
            TupleDomain<Subfield> domainPredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createOrcPageSource(
                ORC,
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                prefilledValues,
                outputColumns,
                domainPredicate,
                useOrcColumnNames,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats,
                domainCompactionThreshold));
    }

    public static OrcSelectivePageSource createOrcPageSource(
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
            List<Integer> outputColumns,
            TupleDomain<Subfield> domainPredicate,
            boolean useOrcColumnNames,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold)
    {
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");

        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            OrcReader reader = new OrcReader(orcDataSource, orcEncoding, maxMergeDistance, maxBufferSize, tinyStripeThreshold, maxReadBlockSize);

            checkArgument(!domainPredicate.isNone(), "Unexpected NONE domain");

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
            Map<Integer, Integer> indexMapping = IntStream.range(0, columns.size())
                    .boxed()
                    .collect(toImmutableMap(i -> columns.get(i).getHiveColumnIndex(), i -> physicalColumns.get(i).getHiveColumnIndex()));

            Map<Integer, String> columnNames = physicalColumns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, HiveColumnHandle::getName));

            OrcPredicate orcPredicate = toOrcPredicate(domainPredicate, physicalColumns, typeManager, domainCompactionThreshold, orcBloomFiltersEnabled);

            Map<Integer, TupleDomainFilter> tupleDomainFilters = toTupleDomainFilters(domainPredicate, ImmutableBiMap.copyOf(columnNames).inverse());

            Map<Integer, List<Subfield>> requiredSubfields = physicalColumns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, HiveColumnHandle::getRequiredSubfields));

            Map<Integer, Type> columnTypes = physicalColumns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, column -> typeManager.getType(column.getTypeSignature())));

            Map<Integer, Object> typedPrefilledValues = Maps.transformEntries(
                    prefilledValues.entrySet().stream()
                            .collect(toImmutableMap(entry -> indexMapping.get(entry.getKey()), Map.Entry::getValue)),
                    (hiveColumnIndex, value) -> typedPartitionKey(value, columnTypes.get(hiveColumnIndex), columnNames.get(hiveColumnIndex), hiveStorageTimeZone));

            OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                    columnTypes,
                    outputColumns.stream().map(indexMapping::get).collect(toImmutableList()),
                    tupleDomainFilters,
                    requiredSubfields,
                    typedPrefilledValues,
                    orcPredicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    Optional.empty(),
                    INITIAL_BATCH_SIZE);

            return new OrcSelectivePageSource(
                    recordReader,
                    orcDataSource,
                    systemMemoryUsage.newAggregatedMemoryContext(),
                    stats);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static Map<Integer, TupleDomainFilter> toTupleDomainFilters(TupleDomain<Subfield> domainPredicate, Map<String, Integer> columnIndices)
    {
        // TODO Add support for filters on subfields
        checkArgument(domainPredicate.getDomains().get().keySet().stream()
                .allMatch(OrcSelectivePageSourceFactory::isEntireColumn), "Filters on subfields are not supported yet");

        return Maps.transformValues(
                domainPredicate.transform(subfield -> isEntireColumn(subfield) ? columnIndices.get(subfield.getRootName()) : null)
                        .getDomains()
                        .get(),
                TupleDomainFilterUtils::toFilter);
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static OrcPredicate toOrcPredicate(TupleDomain<Subfield> domainPredicate, List<HiveColumnHandle> physicalColumns, TypeManager typeManager, int domainCompactionThreshold, boolean orcBloomFiltersEnabled)
    {
        ImmutableList.Builder<TupleDomainOrcPredicate.ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
        for (HiveColumnHandle column : physicalColumns) {
            if (column.getColumnType() == REGULAR) {
                Type type = typeManager.getType(column.getTypeSignature());
                columnReferences.add(new TupleDomainOrcPredicate.ColumnReference<>(column, column.getHiveColumnIndex(), type));
            }
        }

        Map<String, HiveColumnHandle> columnsByName = uniqueIndex(physicalColumns, HiveColumnHandle::getName);
        TupleDomain<HiveColumnHandle> entireColumnDomains = domainPredicate.transform(subfield -> isEntireColumn(subfield) ? columnsByName.get(subfield.getRootName()) : null);
        return new TupleDomainOrcPredicate<>(entireColumnDomains, columnReferences.build(), orcBloomFiltersEnabled, Optional.of(domainCompactionThreshold));
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }
}
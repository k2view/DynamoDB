package com.k2view.cdbms.usercode.common.dynamodb.metadata;

import com.k2view.cdbms.usercode.common.dynamodb.DynamoDBIoSession;
import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.model.impl.DatasetEntry;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings("all")
public class DynamoDbSnapshotDataset implements SnapshotDataset {
    private final Log log = Log.a(this.getClass());
    private final String dataset;
    private final SampleSize size;
    private final DynamoDbClient dbClient;
    private final DynamoDBIoSession session;

    public DynamoDbSnapshotDataset(DynamoDBIoSession session, DynamoDbClient dbClient, String dataset, SampleSize size) {
        this.session = session;
        this.dbClient = dbClient;
        this.dataset=dataset;
        this.size=size;
    }

    @Override
    public Iterator<Map<String, Object>> fetch() throws Exception {
        final int limit = getLimit(dataset, size);
        String query=String.format("SELECT * from \"%s\"", dataset);
        query = query.concat(" LIMIT ?");
        IoCommand.Result result = session.prepareStatement(query).execute(limit);
        Iterator<IoCommand.Row> iterator = result.iterator();
        return (Iterator<Map<String, Object>>)((Iterator<?>) iterator);
    }

    private int getLimit(String dataset, SampleSize size) {
        int limit;
        long count = getNumberOfRows(dataset);
        int countPercentage = Math.toIntExact(count * size.getPercentage() / 100);
        if (countPercentage < size.getMin()) {
            limit = Math.toIntExact(size.getMin());
        } else if (countPercentage >= size.getMax()) {
            limit = Math.toIntExact(size.getMax());
        } else {
            limit = countPercentage;
        }
        return limit;
    }

    private long getNumberOfRows(String dataset) {
        DescribeTableResponse describeTableResponse = dbClient.describeTable(DescribeTableRequest
                .builder()
                .tableName(dataset)
                .build());
        return describeTableResponse.table().itemCount();
        // Note - itemCount is only updated every 6 hours at AWS
    }

    @Override
    public void close() {}
}

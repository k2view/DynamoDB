package com.k2view.cdbms.usercode.common.dynamodb.metadata;

import com.k2view.cdbms.usercode.common.dynamodb.DynamoDBIoSession;
import com.k2view.discovery.schema.io.IoMetadata;
import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.model.Category;
import com.k2view.discovery.schema.model.DataPlatform;
import com.k2view.discovery.schema.model.impl.*;
import com.k2view.discovery.schema.model.types.BytesClass;
import com.k2view.discovery.schema.model.types.StringClass;
import com.k2view.discovery.schema.model.types.UnknownClass;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;


import java.util.*;
import java.util.stream.Collectors;

import static com.k2view.discovery.crawl.JdbcIoMetadata.EXCLUDE_LIST;
import static com.k2view.discovery.crawl.JdbcIoMetadata.INCLUDE_LIST;

public class DynamoDbMetadata implements IoMetadata {
    private static final String DATASET = "dataset";
    private static final String FIELD = "field";
    private static final String ENTITY_NAME = "entityName";
    private static final String CRAWLER = "Crawler";

    private final String interfaceId;
    private final DynamoDbClient dbClient;
    private final DynamoDBIoSession session;
    private boolean selfCreatedSession;

    private List<String> tablesInclude = new LinkedList<>();
    private List<String> tablesExclude = new LinkedList<>();

    public DynamoDbMetadata(String interfaceIdentifier, DynamoDbClient dbClient, Map<String, Object> props) {
        this(interfaceIdentifier, new DynamoDBIoSession(), dbClient, props);
        this.selfCreatedSession = true;
    }

    @SuppressWarnings("unchecked")
    public DynamoDbMetadata(String interfaceIdentifier, DynamoDBIoSession dynamoDBIoSession, DynamoDbClient dbClient, Map<String, Object> props) {
        this.interfaceId=interfaceIdentifier;
        this.dbClient=dbClient;
        if (!Util.isEmpty(props) && props.containsKey(EXCLUDE_LIST)) {
            tablesExclude = (List<String>) props.get(EXCLUDE_LIST);
        }
        if (!Util.isEmpty(props) && props.containsKey(INCLUDE_LIST)) {
            tablesInclude = (List<String>) props.get(INCLUDE_LIST);
        }
        this.session = dynamoDBIoSession;
    }

    @Override
    public DataPlatform getDataPlatform() {
        ConcreteDataPlatform dataPlatform = addPlatformNode(this.interfaceId);
        ConcreteSchemaNode schemaNode = addSchemaNode(dataPlatform);
        addDatasetNodes(schemaNode);
        return dataPlatform;
    }

    @Override
    public SnapshotDataset snapshotDataset(String dataset, String schema, SampleSize size, Map<String, Object> map) {
        return new DynamoDbSnapshotDataset(session, dbClient, dataset, size);
    }

    private static ConcreteDataPlatform addPlatformNode(String platform) {
        ConcreteDataPlatform dataPlatform = new ConcreteDataPlatform(platform);
        String idPrefix ="dataPlatform:" + dataPlatform.getId();
        dataPlatform.addProperty(idPrefix, ENTITY_NAME, "Data Platform Name", dataPlatform.getName(), 1.0, CRAWLER, "");
        dataPlatform.addProperty(idPrefix, "type", "Data Platform Type", "DynamoDB", 1.0, CRAWLER, "");
        return dataPlatform;
    }

    private ConcreteSchemaNode addSchemaNode(ConcreteDataPlatform dataPlatform) {
        String schemaId = "main";
        ConcreteSchemaNode schemaNode = new ConcreteSchemaNode(schemaId);
        dataPlatform.contains(schemaNode, 1.0, CRAWLER, "");
        schemaNode.addProperty("schema:" + schemaId, ENTITY_NAME, "Name of the schema", schemaId, 1.0, CRAWLER, "");
        return schemaNode;
    }

    private void addDatasetNodes(ConcreteSchemaNode schemaNode) {
        String lastEvaluatedTableName = null;
        do {
            ListTablesRequest.Builder requestBuilder = ListTablesRequest.builder();
            if (lastEvaluatedTableName != null) {
                requestBuilder.exclusiveStartTableName(lastEvaluatedTableName);
            }
            ListTablesResponse response = dbClient.listTables(requestBuilder.build());
            List<String> tableNames = response.tableNames();
            if (!Util.isEmpty(tablesExclude)) {
                tableNames = tableNames.stream().filter(table -> !tablesExclude.contains(table)).collect(Collectors.toList());
            } else if (!Util.isEmpty(tablesInclude)) {
                tableNames = tableNames.stream().filter(table -> tablesInclude.contains(table)).collect(Collectors.toList());
            }
            tableNames.forEach(table -> addDatasetNode(table, schemaNode));
            lastEvaluatedTableName = response.lastEvaluatedTableName();
        } while (lastEvaluatedTableName != null);
    }

    private String idPrefix(String prefix, ConcreteNode node) {
        return prefix + ":" + node.getId();
    }

    private void addDatasetNode(String table, ConcreteSchemaNode schemaNode) {
        ConcreteClassNode datasetClassNode = new ConcreteClassNode(table);
        datasetClassNode.addProperty("class:" + datasetClassNode.getId(), ENTITY_NAME, "Name of the table", table, 1.0, CRAWLER, "");

        ConcreteDataset datasetNode = new ConcreteDataset(table);
        datasetNode.definedBy(datasetClassNode, 1.0, CRAWLER, "");
        datasetNode.addProperty(this.idPrefix(DATASET, datasetNode), ENTITY_NAME, "Name of the table", table, 1.0, CRAWLER, "");
        schemaNode.contains(datasetNode, 1.0, CRAWLER, "");

        DescribeTableRequest describeTableRequest = DescribeTableRequest
                .builder()
                .tableName(table)
                .build();
        TableDescription tableDescription = dbClient.describeTable(describeTableRequest).table();
        addSecondaryIndices(datasetClassNode, tableDescription);
        addOtherTableMetadata(datasetClassNode, tableDescription);
        addFieldNodes(datasetClassNode,
                tableDescription.attributeDefinitions(),
                tableDescription.keySchema());
    }

    private void addSecondaryIndices(ConcreteClassNode datasetClassNode, TableDescription tableDescription) {
        if (tableDescription.hasGlobalSecondaryIndexes()) {
            //        List<Map<Object, Object>> globalSecondaryIndexes = tableDescription.globalSecondaryIndexes()
//                .stream()
//                .map(index ->
//                        Util.map(
//                                "indexName",
//                                index.indexName(),
//                                "indexAttributes",
//                                index
//                                        .keySchema()
//                                        .stream()
//                                        .map(KeySchemaElement::attributeName)
//                                        .collect(Collectors.toList())))
//                .collect(Collectors.toList());
//

            datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                    "globalSecondaryIndices",
                    "Global Secondary Indices",
//                globalSecondaryIndexes,
                    String.valueOf(tableDescription.globalSecondaryIndexes()),
                    1.0,
                    CRAWLER,
                    "");
        }

        if (tableDescription.hasLocalSecondaryIndexes()) {
            //        List<Map<Object, Object>> localSecondaryIndexes = tableDescription.localSecondaryIndexes()
//                .stream()
//                .map(index ->
//                        Util.map(
//                                "indexName",
//                                index.indexName(),
//                                "indexAttributes",
//                                index
//                                        .keySchema()
//                                        .stream()
//                                        .map(KeySchemaElement::attributeName)
//                                        .collect(Collectors.toList())))
//                .collect(Collectors.toList());
            datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                    "localSecondaryIndices",
                    "Local Secondary Indices",
//                localSecondaryIndexes,
                    String.valueOf(tableDescription.globalSecondaryIndexes()),
                    1.0,
                    CRAWLER,
                    "");
        }
    }

    private void addOtherTableMetadata(ConcreteClassNode datasetClassNode, TableDescription tableDescription) {
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "billingModeSummary",
                "Billing Mode Summary",
                String.valueOf(tableDescription.billingModeSummary()),
                1.0,
                CRAWLER,
                "");
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "creationDateTime",
                "Creation Date/Time",
                tableDescription.creationDateTime(),
                1.0,
                CRAWLER,
                "");
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "deletionProtectionEnabled",
                "Deletion Protection Enabled",
                tableDescription.deletionProtectionEnabled(),
                1.0,
                CRAWLER,
                "");
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "provisionedThroughput",
                "ProvisionedThroughput",
                String.valueOf(tableDescription.provisionedThroughput()),
                1.0,
                CRAWLER,
                "");
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "tableSizeBytes",
                "Table size (bytes)",
                tableDescription.tableSizeBytes(),
                1.0,
                CRAWLER,
                "");
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "itemCount",
                "Item count",
                tableDescription.itemCount(),
                1.0,
                CRAWLER,
                "");
        datasetClassNode.addProperty(this.idPrefix(DATASET, datasetClassNode),
                "tableClassSummary",
                "Table Class Summary",
                String.valueOf(tableDescription.tableClassSummary()),
                1.0,
                CRAWLER,
                "");
    }

    private void addFieldNodes(ConcreteClassNode tableClassNode, List<AttributeDefinition> attributeDefinitions, List<KeySchemaElement> keySchemaElements) {
        attributeDefinitions.forEach(attributeDef -> {
            ConcreteField fieldNode = new ConcreteField(attributeDef.attributeName());
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sourceDataType.name(), "Column type", attributeDef.attributeTypeAsString(), 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sourceNullable.name(), "Nullability of the field 1 or 0", "FALSE", 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.sourceEntityType.name(), "Role", "Column", 1.0, CRAWLER, "");
            fieldNode.addProperty(this.idPrefix(FIELD, fieldNode), Category.definedBy.name(), "Data type for field", getFieldDataType(attributeDef),1.0, CRAWLER,"");
            keySchemaElements
                    .stream()
                    .filter(keySchemaElement ->
                            keySchemaElement.attributeName()
                                    .equals(attributeDef.attributeName()))
                    .findFirst()
                    .ifPresent(keySchemaElement -> fieldNode.addProperty(
                            this.idPrefix(FIELD, fieldNode),
                            "pk",
                            String.format("Primary Key (%s)", keySchemaElement.keyType().toString()),
                            true,
                            1.0,
                            CRAWLER,
                            ""));
            tableClassNode.contains(fieldNode, 1.0, CRAWLER, "");
        });

    }

    public static String getFieldDataType(AttributeDefinition attributeDefinition) {
        switch (attributeDefinition.attributeType()) {
            case S:
                return StringClass.STRING.getClassName();
            case B:
                return BytesClass.BYTES.getClassName();
            case N:
                // TO-DO
                return "BIG_DECIMAL";
            default:
                return UnknownClass.UNKNOWN.getClassName();
        }
    }

    @Override
    public void close() {
        if (selfCreatedSession) {
            Util.safeClose(session);
        }
    }
}

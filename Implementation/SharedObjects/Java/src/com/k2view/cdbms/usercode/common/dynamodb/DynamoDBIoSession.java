package com.k2view.cdbms.usercode.common.dynamodb;

import com.k2view.cdbms.usercode.common.dynamodb.metadata.DynamoDbMetadata;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoSession;
import com.k2view.fabric.common.io.basic.IoSimpleResultSet;
import com.k2view.fabric.common.io.basic.IoSimpleRow;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import javax.annotation.concurrent.GuardedBy;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.k2view.cdbms.usercode.common.dynamodb.DynamoDBParseUtils.toAttributeValueList;

@SuppressWarnings("all")
public class DynamoDBIoSession extends AbstractIoSession {
    private final Log log = Log.a(this.getClass());
    private boolean inTransaction;

    private final Object transactionStatementsLock = new Object();
    @GuardedBy("transactionStatementsLock")
    private List<ParameterizedStatement> transactionStatements;

    private final Object batchStatementsLock = new Object();
    @GuardedBy("batchStatementsLock")
    private List<BatchStatementRequest> batchStatementRequests;

    private final int recordsInBatch;
    private final String interfaceIdentifier;

    private DynamoDbClient dbClient;
    private final Map<String, Object> sessionParams;

    public DynamoDBIoSession() {
        this(null, null);
    }

    public DynamoDBIoSession(String identifier, Map<String, Object> params) {
        log.debug("Creating DynamoDBIoSession {}", this);
        this.sessionParams = new HashMap<>();
        if (params != null) this.sessionParams.putAll(params);
        this.inTransaction = false;
        this.interfaceIdentifier = identifier;
        this.recordsInBatch = ParamConvertor.toNumber(params.get("BATCH_SIZE")).intValue();
        if (recordsInBatch < 1) {
            throw new IllegalArgumentException("Batch size must be between 1 and the maximum defined by AWS");
        }
        this.dbClient = this.createDbClient();
    }

    private DynamoDbClient createDbClient() {
        Object region = sessionParams.get("REGION");
        DynamoDbClientBuilder dynamoDbClientBuilder = DynamoDbClient
                .builder()
                .credentialsProvider(DefaultCredentialsProvider.create());
        if (region != null && !Util.isEmpty(region.toString())) {
            dynamoDbClientBuilder.region(Region.of(region.toString().toLowerCase()));
        }
        return dynamoDbClientBuilder.build();
    }

    @Override
    public void close() {
        log.debug("Closing DynamoDBIoSession {}", this);
        Util.safeClose(dbClient);
        dbClient=null;
        batchStatementRequests=null;
        transactionStatements=null;
    }

    @Override
    public Statement statement(){
        log.debug("Creating DynamoDB statement");
        return new DynamoDBStatement();
    }

    @Override
    public Statement prepareStatement(String command) {
        log.debug("Creating DynamoDB prepared statement");
        return new DynamoDBPreparedStatement(command);
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public void commit() {
        log.debug("Committing DynamoDB transaction");
        if (transactionStatements != null && !transactionStatements.isEmpty()) {
            ExecuteTransactionRequest executeTransactionRequest = ExecuteTransactionRequest.builder().transactStatements(transactionStatements).build();
            dbClient.executeTransaction(executeTransactionRequest);
            transactionStatements.clear();
        }
        if (batchStatementRequests != null && !batchStatementRequests.isEmpty()) {
            executeBatch();
        }
        inTransaction=false;
    }

    @Override
    public void beginTransaction() {
        inTransaction = true;
    }

    @Override
    public void abort() {
        log.debug("Aborting DynamoDB session");
        if (batchStatementRequests!=null) {
            batchStatementRequests.clear();
        }
        if (transactionStatements != null) {
            transactionStatements.clear();
        }
        inTransaction=false;
    }

    @Override
    public void testConnection() {
        dbClient.listTables();
    }

    @Override
    public IoSession.IoSessionCompartment compartment() {
        return IoSessionCompartment.SHARED;
    }

    private void executeBatch() {
        log.debug("Executing batch");
        BatchExecuteStatementRequest request = BatchExecuteStatementRequest.builder().statements(batchStatementRequests).build();
        batchStatementRequests.clear();
        dbClient.batchExecuteStatement(request);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(Map<String, Object> params) {
        return (T) new DynamoDbMetadata(interfaceIdentifier, this, dbClient, params);
    }

    private abstract class AbstractDynamoDBStatement implements IoCommand.Statement {
        private final Log log = Log.a(this.getClass());
        protected Integer limit;

        IoCommand.Result execute(String command, List<AttributeValue> parameters) {
            log.debug("Executing DynamoDb command={}, params={}", command, parameters);
            if (!inTransaction) {
                try (ProcessCommandWithLimit processCommandWithLimit = new ProcessCommandWithLimit().process(command, parameters)) {
                    this.limit = processCommandWithLimit.limit;
                    ExecuteStatementRequest.Builder requestBuilder = ExecuteStatementRequest.builder()
                            .statement(processCommandWithLimit.commandWithoutLimit)
                            .limit(limit);
                    if (!Util.isEmpty(processCommandWithLimit.paramsWithoutLimit)) {
                        requestBuilder.parameters(processCommandWithLimit.paramsWithoutLimit);
                    }
                    ExecuteStatementResponse executeStatementResponse = dbClient.executeStatement(requestBuilder.build());
                    if (limit != null) limit = limit - executeStatementResponse.items().size();
                    return new DynamoDBResult(
                            executeStatementResponse,
                            processCommandWithLimit.commandWithoutLimit,
                            processCommandWithLimit.paramsWithoutLimit,
                            limit);
                }
            } else {
                if (command!=null && command.toLowerCase().startsWith("select")) {
                    throw new UnsupportedOperationException("Select statements in transactions are unsupported!");
                }
                ParameterizedStatement parameterizedStatement = ParameterizedStatement.builder().statement(command)
                        .parameters(parameters).build();
                synchronized (transactionStatementsLock) {
                    if (transactionStatements == null) {
                        transactionStatements = Collections.synchronizedList(new ArrayList<>());
                    }
                }
                transactionStatements.add(parameterizedStatement);
                return new IoSimpleResultSet(-1);
            }
        }

        void batch(String command, List<AttributeValue> parameters) {
            log.debug("In statement batch()");
            if (!inTransaction) {
                throw new UnsupportedOperationException("Batch mode outside of a transaction is not allowed!");
            }
            if (command!=null && command.toLowerCase().startsWith("select")) {
                throw new UnsupportedOperationException("Batch select statements are unsupported!");
            }
            synchronized (batchStatementsLock) {
                if (batchStatementRequests == null) {
                    batchStatementRequests = Collections.synchronizedList(new ArrayList<>());
                }
            }
            BatchStatementRequest statementRequest = BatchStatementRequest.builder().statement(command).parameters(parameters).build();
            batchStatementRequests.add(statementRequest);
            synchronized (batchStatementsLock) {
                if (batchStatementRequests.size() >= recordsInBatch) {
                    executeBatch();
                }
            }
        }

        private class DynamoDBResult implements IoCommand.Result {
            private final ExecuteStatementResponse executeStatementResponse;
            private final String command;
            private final List<AttributeValue> params;
            private Integer limit;

            public DynamoDBResult(ExecuteStatementResponse executeStatementResponse, String command, List<AttributeValue> params, Integer limit) {
                this.executeStatementResponse = executeStatementResponse;
                this.command = command;
                this.params = params;
                this.limit = limit;
            }

            @Override
            public int rowsAffected() throws Exception {
                return IoCommand.Result.super.rowsAffected();
            }

            @Override
            public String[] labels() {
                // Each item in response may have different attributes/fields
                return new String[]{};
            }

            @Override
            public Iterator<IoCommand.Row> iterator() {
                return new DynamoDBIterator(executeStatementResponse);
            }

            private class DynamoDBIterator implements Iterator<IoCommand.Row> {
                private ExecuteStatementResponse response;
                private Iterator<Map<String, AttributeValue>> responseIterator;
                public DynamoDBIterator(ExecuteStatementResponse response) {
                    this.setResponse(response);
                }

                @Override
                public boolean hasNext() {
                    if (responseIterator.hasNext()) return true;
                    return this.shouldFetchNext();
                }

                @Override
                public IoCommand.Row next() {
                    if (responseIterator.hasNext()) return this.translate(this.responseIterator.next());
                    if (this.shouldFetchNext()) {
                        ExecuteStatementRequest nextRequest = ExecuteStatementRequest.builder()
                                .statement(command)
                                .parameters(params)
                                .nextToken(response.nextToken())
                                .limit(limit)
                                .build();
                        this.setResponse(dbClient.executeStatement(nextRequest));
                        if (limit != null) {
                            limit = limit - this.response.items().size();
                        }
                        return this.next();
                    }
                    return null;
                }

                private IoCommand.Row translate(Map<String, AttributeValue> item) {
                    Map<String, Object> parsedItem = new LinkedHashMap<>();
                    Map<String, Integer> keys = new LinkedHashMap<>();
                    item.forEach((key, value) -> parsedItem.put(key, DynamoDBParseUtils.fromAttributeValue(value)));
                    Iterator<String> keySetItr = parsedItem.keySet().iterator();
                    for (int i = 0; i < parsedItem.keySet().size(); i++) {
                        keys.put(keySetItr.next(), i);
                    }
                    return new IoSimpleRow(parsedItem.values().toArray(), keys);
                }

                private void setResponse(ExecuteStatementResponse response) {
                   this.response = response;
                   this.responseIterator = response.hasItems() ?
                            response.items().iterator()
                            : Collections.emptyIterator();
                }

                private boolean shouldFetchNext() {
                    return response.nextToken() != null && (limit == null || limit > 0);
                }
            }
        }
    }

    private class DynamoDBPreparedStatement extends DynamoDBIoSession.AbstractDynamoDBStatement {
        private final String command;

        public DynamoDBPreparedStatement(String command) {
            super();
            this.command = command;
        }

        @Override
        public IoCommand.Result execute(Object... objects) {
            return super.execute(command, toAttributeValueList(objects));
        }

        @Override
        public void batch(Object... params) {
            super.batch(command, toAttributeValueList(params));
        }
    }

    private class DynamoDBStatement extends DynamoDBIoSession.AbstractDynamoDBStatement {
        @Override
        public IoCommand.Result execute(Object... objects) {
            if (objects.length != 1) {
                throw new IllegalArgumentException("A Statement must have exactly one parameter which is the PartiQL command");
            }
            String command = (String) objects[0];
            return super.execute(command, null);
        }

        @Override
        public void batch(Object... params) {
            if (params.length != 1) {
                throw new IllegalArgumentException("A Statement must have exactly one parameter which is the command");
            }
            String command = (String) params[0];
            super.batch(command, null);
        }
    }

    private static class ProcessCommandWithLimit implements AutoCloseable {
        private String commandWithoutLimit;
        private Integer limit;
        private List<AttributeValue> paramsWithoutLimit;
        private final Pattern pattern;
        ProcessCommandWithLimit() {
            this.pattern = Pattern.compile("(.*)\\blimit\\b\\s*(\\d+|\\?)\\s*$", Pattern.CASE_INSENSITIVE);
        }
        private ProcessCommandWithLimit process(String origStatement, List<AttributeValue> parameters) {
            Matcher matcher = pattern.matcher(origStatement);
            this.paramsWithoutLimit = new LinkedList<>(parameters);
            this.commandWithoutLimit = origStatement;
            if (matcher.find()) {
                String beforeLimit = matcher.group(1);
                String numOrQuestionMark = matcher.group(2);
                String limitNum = numOrQuestionMark;
                if (numOrQuestionMark.equals("?")) {
                    limitNum = paramsWithoutLimit.remove(parameters.size()-1).n();
                }
                this.limit = Integer.parseInt(limitNum);
                this.commandWithoutLimit=beforeLimit;
            }
            return this;
        }

        @Override
        public void close() {
            if (!Util.isEmpty(this.paramsWithoutLimit)) this.paramsWithoutLimit.clear();
        }
    }

}

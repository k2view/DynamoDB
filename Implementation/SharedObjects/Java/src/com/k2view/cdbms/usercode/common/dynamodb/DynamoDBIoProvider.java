package com.k2view.cdbms.usercode.common.dynamodb;

import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

import java.util.Map;

public class DynamoDBIoProvider implements IoProvider {
    @Override
    public IoSession createSession(String identifier, Map<String, Object> params) {
        Map<String,Object> data = Json.get().fromJson(params.get("Data").toString());
        if (!Util.isEmpty(data)) {
            params.putAll(data);
        }
        return new DynamoDBIoSession(identifier, params);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IoProvider> T unwrap(Class<T> aClass) {
        return (T) this;
    }

}

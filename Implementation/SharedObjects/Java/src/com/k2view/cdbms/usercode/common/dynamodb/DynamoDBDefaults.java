package com.k2view.cdbms.usercode.common.dynamodb;

import java.util.Map;

public enum DynamoDBDefaults {
    BATCH_SIZE(25);

    private final Object defaultVal;
    DynamoDBDefaults(Object defaultVal) {
        this.defaultVal=defaultVal;
    }

    public String getName() {
        return this.name();
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Map<String, Object> args) {
        if (args == null) {
            return (T) this.defaultVal;
        } else {
            Object value = args.get(this.getName());
            if (value == null) {
                value = args.get(this.getName().toLowerCase());
            }

            return (T) (value == null ? this.defaultVal : value);
        }
    }
}

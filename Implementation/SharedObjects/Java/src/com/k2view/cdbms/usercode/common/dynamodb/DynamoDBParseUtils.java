package com.k2view.cdbms.usercode.common.dynamodb;

import com.k2view.fabric.common.ByteStream;
import com.k2view.fabric.common.ParamConvertor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.string.BigDecimalStringConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.k2view.fabric.common.ParamConvertor.toBuffer;

/**
 * Provides utility functions for parsing to/from AttributeValue
 */
public class DynamoDBParseUtils {
    private DynamoDBParseUtils() {}

    /**
     * @param value The object to be parsed
     * @return The object parsed to AttributeValue, or null if it needs to be skipped.
     * null will be returned in case value is an empty set.
     */
    protected static AttributeValue toAttributeValue(Object value, Function<Object, AttributeValue> fallout) {
        AttributeValue.Builder attributeValueBuilder = AttributeValue.builder();
        if (value == null) {
            attributeValueBuilder.nul(true);
        } else if (value instanceof String) {
            attributeValueBuilder.s((String) value);
        } else if (value instanceof Number) {
            attributeValueBuilder.n(String.valueOf(value));
        } else if (value instanceof byte[]
        || value instanceof ByteStream
        || value instanceof ByteBuffer
        || value instanceof Blob) {
            attributeValueBuilder.b(toSdkBytes(toBuffer(value)));
        } else if (value instanceof Boolean) {
            attributeValueBuilder.bool((Boolean) value);
        } else if (value instanceof Set<?>) {
            return attributeValueFromSet((Set<?>) value, fallout);
        } else if (value instanceof List) {
            List<?> valueAsList = (List<?>) value;
            List<AttributeValue> parsedList = new LinkedList<>();
            valueAsList.stream().map(innerValue -> toAttributeValue(innerValue, fallout))
                    .filter(DynamoDBParseUtils::notEmptyAttributeValue)
                    .forEach(parsedList::add);
            attributeValueBuilder.l(parsedList);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> valueAsMap = (Map<String, Object>) value;
            Map<String, AttributeValue> parsedMap = valueAsMap.entrySet().stream()
                    .map(entry -> new Object[]{entry.getKey(), toAttributeValue(entry.getValue(), fallout)})
                    .filter(arr -> notEmptyAttributeValue((AttributeValue) arr[1]))
                    .collect(Collectors.toMap(arr -> (String)arr[0], arr -> (AttributeValue)arr[1]));
            attributeValueBuilder.m(parsedMap);
        } else if(value instanceof Date) {
            attributeValueBuilder.s(ParamConvertor.toString(value));
        }
        else if (value instanceof Iterable<?>) {
            Iterator<?> valueAsIterator = ((Iterable<?>) value).iterator();
            List<AttributeValue> parsedIterable = new LinkedList<>();
            while(valueAsIterator.hasNext()) {
                AttributeValue attributeValue = toAttributeValue(valueAsIterator.next(), fallout);
                if (notEmptyAttributeValue(attributeValue)) parsedIterable.add(attributeValue);
            }
            attributeValueBuilder.l(parsedIterable);
        } else if(value.getClass().isArray()) {
            List<AttributeValue> parsedArray = Arrays.stream((Object[]) value).map(
                    innerValue -> toAttributeValue(innerValue, fallout))
                    .filter(DynamoDBParseUtils::notEmptyAttributeValue)
                    .collect(Collectors.toList());
            attributeValueBuilder.l(parsedArray);
        } else {
            return fallout.apply(value);
        }
        return attributeValueBuilder.build();
    }

    private static AttributeValue attributeValueFromSet(Set<?> set, Function<Object, AttributeValue> fallout) {
        AttributeValue.Builder attributeValueBuilder = AttributeValue.builder();
        Iterator<?> setItr = set.iterator();
        if (!setItr.hasNext()) {
            return fallout.apply(set);
//          DynamoDB does not accept an empty Set
        }
        Class<?> firstItemClass = setItr.next().getClass();
        if (firstItemClass == String.class) {
            @SuppressWarnings("unchecked") Set<String> strSet = (Set<String>) set;
            attributeValueBuilder.ss(strSet);
        } else if (firstItemClass == byte[].class
                || firstItemClass == ByteStream.class
                || firstItemClass == Blob.class
                || firstItemClass == ByteBuffer.class) {
            Set<SdkBytes> sdkBytesSet = new HashSet<>();
            set.forEach(val ->
                    sdkBytesSet.add(toSdkBytes(toBuffer(val))));
            attributeValueBuilder.bs(sdkBytesSet);
        } else if (Number.class.isAssignableFrom(firstItemClass)) {
            Set<String> stringSet = new HashSet<>();
            set.forEach(val -> stringSet.add(String.valueOf(val)));
            attributeValueBuilder.ns(stringSet);
        } else if (Date.class.isAssignableFrom(firstItemClass)) {
            Set<String> stringSet = new HashSet<>();
            set.forEach(val -> stringSet.add(ParamConvertor.toString(val)));
            attributeValueBuilder.ss(stringSet);
        } else {
            return fallout.apply(set);
        }
        return attributeValueBuilder.build();
    }

    private static SdkBytes toSdkBytes(byte[] byteArr) {
        return SdkBytes.fromByteArray(Base64.getEncoder().encode(byteArr));
    }

    /**
     * @param value An AttributeValue object
     * @return The object converted to an equivalent type
     * that is supported by fabric
     */
    protected static Object fromAttributeValue(AttributeValue value) {
        switch (value.type()) {
            case S:
                return value.s();
            case N:
                BigDecimalStringConverter bigDecimalStringConverter = BigDecimalStringConverter.create();
                return bigDecimalStringConverter.fromString(value.n());
                // TODO - Double/Long or BigDecimal?
//                return ParamConvertor.toNumber(value.n());
            case BOOL:
                return value.bool();
            case B:
                return value.b().asByteArray();
            case NUL:
                return null;
            case L:
                List<Object> parsedList = new LinkedList<>();
                value.l().forEach(listItem -> parsedList.add(fromAttributeValue(listItem)));
                return parsedList;
            case M:
                Map<String, Object> parsedMap = new LinkedHashMap<>();
                value.m().forEach((mapKey, mapVal) -> parsedMap.put(mapKey, fromAttributeValue(mapVal)));
                return parsedMap;
            case BS:
                Set<byte[]> parsedByteSet = new HashSet<>();
                value.bs().forEach(sdkBytes -> parsedByteSet.add(sdkBytes.asByteArray()));
                return parsedByteSet;
            case NS:
                Set<BigDecimal> parsedNumberSet = new HashSet<>();
                value.ns().forEach(number -> parsedNumberSet.add(new BigDecimal(number)));
                return parsedNumberSet;
            case SS:
                return new HashSet<>(value.ss());
            default:
                throw new IllegalArgumentException("Encountered unknown AttributeValue type for " + value);
        }
    }

    /**
     * @param params The array of parameters to be parsed
     * @return A list of AttributeValue objects
     */
    protected static List<AttributeValue> toAttributeValueList(Object... params) {
        Function<Object, AttributeValue> fallout = param -> {
            if(param instanceof Set<?>) {
                Set<?> set = (Set<?>) param;
                if (set.isEmpty()) return null;
                // Skip empty sets instead of throwing exception
                // as they're not supported by DynamoDB
                throw new IllegalArgumentException(String.format("Unsupported type of Set: Set<%s>",
                        set.iterator().next().getClass().getName()));
            }
            throw new IllegalArgumentException(String.format("Unsupported param of type %s", param.getClass().getName()));
        };
        if (params == null || params.length == 0) {
            return Collections.singletonList(DynamoDBParseUtils.toAttributeValue(null, fallout));
        }
        return Arrays.stream(params).map(param -> DynamoDBParseUtils.toAttributeValue(param, fallout))
                .filter(Objects::nonNull).collect(Collectors.toList());
    }

    protected static boolean notEmptyAttributeValue(AttributeValue val) {
        if(val == null) return false;
        return !val.toBuilder().equals(AttributeValue.builder());
    }
}

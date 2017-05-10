package reactive.mongo.codec.tmp;

import org.bson.*;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivecouchbase.json.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsObjectCodecProvider implements CodecProvider {
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP;

    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();

    /**
     * Construct a new instance with the default codec for each BSON type.
     */
    public JsObjectCodecProvider() {
        addCodecs();
    }

    /**
     * Get the {@code JsValue} subclass associated with the given {@code BsonType}.
     * @param bsonType the BsonType
     * @return the class associated with the given type
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends JsValue> getClassForBsonType(final BsonType bsonType) {
        return (Class<? extends JsValue>) DEFAULT_BSON_TYPE_CLASS_MAP.get(bsonType);
    }

    /**
     * Gets the BsonTypeClassMap used by this provider.
     *
     * @return the non-null BsonTypeClassMap
     * @since 3.3
     */
    public static BsonTypeClassMap getBsonTypeClassMap() {
        return DEFAULT_BSON_TYPE_CLASS_MAP;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        if (clazz == JsArray.class) {
            return (Codec<T>) new JsArrayCodec(registry);
        }

//        if (clazz == BsonJavaScriptWithScope.class) {
//            return (Codec<T>) new BsonJavaScriptWithScopeCodec(registry.get(BsonDocument.class));
//        }

        if (clazz == JsValue.class) {
            //return (Codec<T>) new BsonValueCodec(registry);
            return (Codec<T>) new JsValueCodecTmp(registry);
        }
//
//        if (clazz == BsonDocumentWrapper.class) {
//            return (Codec<T>) new BsonDocumentWrapperCodec(registry.get(BsonDocument.class));
//        }
//
//        if (clazz == RawBsonDocument.class) {
//            return (Codec<T>) new RawBsonDocumentCodec();
//        }

        if (JsObject.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new JsObjectCodec(registry);
        }

        return null;
    }

    private void addCodecs() {
        addCodec(new JsNumberCodec());
        addCodec(new JsBooleanCodec());
        addCodec(new JsStringCodec());
        addCodec(new JsNullCodec());
        addCodec(new JsUndefinedCodec());
    }

    private <T extends JsValue> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }

    static {
        Map<BsonType, Class<?>> map = new HashMap<BsonType, Class<?>>();

        map.put(BsonType.NULL, JsNull.class);
        map.put(BsonType.ARRAY, JsArray.class);
        map.put(BsonType.BINARY, JsString.class);
        map.put(BsonType.BOOLEAN, JsBoolean.class);
        map.put(BsonType.DATE_TIME, JsObject.class);
        map.put(BsonType.DB_POINTER, JsObject.class); //BsonDbPointer;
        map.put(BsonType.DOCUMENT, JsObject.class);
        map.put(BsonType.DOUBLE, JsNumber.class); //BsonDouble;
        map.put(BsonType.INT32, JsNumber.class);//BsonInt32
        map.put(BsonType.INT64, JsNumber.class);//BsonInt64
        map.put(BsonType.DECIMAL128, JsNumber.class);//BsonDecimal128
        map.put(BsonType.MAX_KEY, JsString.class);
        map.put(BsonType.MIN_KEY, JsString.class);
        map.put(BsonType.JAVASCRIPT, BsonJavaScript.class);
        map.put(BsonType.JAVASCRIPT_WITH_SCOPE, BsonJavaScriptWithScope.class);
        map.put(BsonType.OBJECT_ID, JsObject.class); //BsonObjectId
        map.put(BsonType.REGULAR_EXPRESSION, JsString.class);//BsonRegularExpression
        map.put(BsonType.STRING, JsString.class);
        map.put(BsonType.SYMBOL, JsString.class);//BsonSymbol
        map.put(BsonType.TIMESTAMP, JsObject.class); //BsonTimestamp
        map.put(BsonType.UNDEFINED, JsUndefined.class);

        DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap(map);
    }
}

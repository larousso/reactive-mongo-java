package reactive.mongo.codec.tmp;

import javaslang.Tuple2;
import javaslang.control.Try;
import org.bson.*;
import org.bson.codecs.BsonTypeCodecMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.reactivecouchbase.json.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Predicates.instanceOf;
import static org.bson.codecs.BsonValueCodecProvider.getBsonTypeClassMap;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * Created by adelegue on 09/05/2017.
 */
public class JsObjectCodec implements Codec<JsObject> {
    private static final String ID_FIELD_NAME = "_id";
    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(new JsObjectCodecProvider());

    private final CodecRegistry codecRegistry;
    private final BsonTypeCodecMap bsonTypeCodecMap;

    /**
     * Creates a new instance with a default codec registry that uses the {@link JsObjectCodecProvider}.
     */
    public JsObjectCodec() {
        this(DEFAULT_REGISTRY);
    }

    /**
     * Creates a new instance initialised with the given codec registry.
     *
     * @param codecRegistry the {@code CodecRegistry} to use to look up the codecs for encoding and decoding to/from BSON
     */
    public JsObjectCodec(final CodecRegistry codecRegistry) {
        if (codecRegistry == null) {
            throw new IllegalArgumentException("Codec registry can not be null");
        }
        this.codecRegistry = codecRegistry;
        this.bsonTypeCodecMap = new BsonTypeCodecMap(getBsonTypeClassMap(), codecRegistry);
    }

    JsObject toId(ObjectId objectId) {
        return Json.obj().with("$oid", objectId.toString());
    }

    @Override
    public JsObject decode(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType currentBsonType = reader.getCurrentBsonType();
        return Match(currentBsonType).of(
                Case($(BsonType.DATE_TIME), any -> {
                    long dateTime = reader.readDateTime();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    return Json.obj().with("$date", String.valueOf(dateTime));
                }),
                Case($(BsonType.DB_POINTER), any -> {
                    BsonDbPointer bsonDbPointer = reader.readDBPointer();
                    ObjectId id = bsonDbPointer.getId();
                    return toId(id);
                }),
                Case($(BsonType.OBJECT_ID), any -> {
                    ObjectId objectId = reader.readObjectId();
                    return toId(objectId);
                }),
                Case($(BsonType.TIMESTAMP), any -> {
                    BsonTimestamp bsonTimestamp = reader.readTimestamp();
                    return Json.obj().with("$timestamp", Json.obj().with("t", Integer.toString(bsonTimestamp.getTime())).with("i", bsonTimestamp.getInc()));
                }),
                Case($(BsonType.DOCUMENT), any -> {
                    List<JsPair> keyValuePairs = new ArrayList<>();

                    reader.readStartDocument();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        keyValuePairs.add(new JsPair(fieldName, readValue(reader, decoderContext)));
                    }

                    reader.readEndDocument();

                    return Json.obj(keyValuePairs);
                })
        );
    }

    protected JsValue readValue(final BsonReader reader, final DecoderContext decoderContext) {
        return (JsValue) bsonTypeCodecMap.get(reader.getCurrentBsonType()).decode(reader, decoderContext);
    }
//
//
//    @Override
//    public JsValue decode(BsonReader reader, DecoderContext decoderContext) {
//        BsonType currentBsonType = reader.getCurrentBsonType();
//        return Match(currentBsonType).of(
//          Case($(BsonType.BOOLEAN), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.ARRAY), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.BINARY), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.DATE_TIME), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.DB_POINTER), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.DECIMAL128), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.DOCUMENT), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.DOUBLE), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.END_OF_DOCUMENT), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.BOOLEAN), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.BOOLEAN), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.BOOLEAN), t -> new JsBoolean(reader.readBoolean())),
//          Case($(BsonType.BOOLEAN), t -> new JsBoolean(reader.readBoolean())),
//        );
//    }
//
//    @Override
//    public void encode(BsonWriter writer, JsValue json, EncoderContext encoderContext) {
//        Match(json).of(
//                Case(instanceOf(JsObject.class), jsObject -> {
//                    if(jsObject.fieldAsOpt("$date").isDefined()) {
//                        writer.writeDateTime(dateTime(jsObject).getTime());
//                    } else if(jsObject.fieldAsOpt("$oid").isDefined()) {
//                        writer.writeObjectId(objectId(jsObject));
//                    } else {
//                        writer.writeStartDocument();
//                        jsObject.values.forEach((name, value) -> {
//                            encode(Option.some(name), writer, json, encoderContext);
//                        });
//                        writer.writeEndDocument();
//                    }
//                    return Done.getInstance();
//                }),
//                Case($(), any -> {
//                    encode(Option.none(), writer, json, encoderContext);
//                    return Done.getInstance();
//                })
//        );
//    }

    private ObjectId objectId(JsObject jsObject) {
        return new ObjectId(jsObject.field("$oid").asString());
    }

    private Date dateTime(JsObject jsObject) {
        JsValue aDate = jsObject.field("$date");
        return Match(aDate).of(
                Case(instanceOf(JsNumber.class), jsNumber -> {
                    return new Date(jsNumber.value.longValue());
                }),
                Case(instanceOf(JsString.class), str -> {
                    String $date = aDate.asString();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    return Try.of(() -> dateFormat.parse($date)).get();
                })
        );
    }


    @Override
    public void encode(final BsonWriter writer, final JsObject value, final EncoderContext encoderContext) {

        if(value.fieldAsOpt("$oid").isDefined()) {

        } else if(value.fieldAsOpt("$timestamp").isDefined()) {

        } else if(value.fieldAsOpt("$date").isDefined()) {

        } else {
            writer.writeStartDocument();

            beforeFields(writer, encoderContext, value);
            for (Tuple2<String, JsValue> entry : value.values) {
                if (skipField(encoderContext, entry._1)) {
                    continue;
                }

                writer.writeName(entry._1);
                writeValue(writer, encoderContext, entry._2);
            }

            writer.writeEndDocument();
        }
    }

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final JsObject value) {
        if (encoderContext.isEncodingCollectibleDocument() && value.fieldAsOpt(ID_FIELD_NAME).isDefined()) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, encoderContext, value.field(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValue(final BsonWriter writer, final EncoderContext encoderContext, final JsValue value) {
        Codec codec = codecRegistry.get(value.getClass());
        encoderContext.encodeWithChildContext(codec, writer, value);
    }

//
//    public void encode(Option<String> name, BsonWriter writer, JsValue json, EncoderContext encoderContext) {
//        Match(json).of(
//                Case(instanceOf(JsArray.class), array -> {
//                    name.map(n -> {
//                        writer.writeStartArray(n);
//                        return Done.getInstance();
//                    }).getOrElse(() -> {
//                        writer.writeStartArray();
//                        return Done.getInstance();
//                    });
//                    array.forEach(jsValue ->
//                            encode(name, writer, jsValue, encoderContext)
//                    );
//                    writer.writeEndArray();
//                    return Done.getInstance();
//                }),
//                Case(instanceOf(JsBoolean.class), bool -> {
//                    name.map(n -> {
//                        writer.writeBoolean(n, bool.value);
//                        return Done.getInstance();
//                    }).getOrElse(() -> {
//                        writer.writeBoolean(bool.value);
//                        return Done.getInstance();
//                    });
//                    return Done.getInstance();
//                }),
//                Case(instanceOf(JsNull.class), jsNull -> {
//                    name.map(n -> {
//                        writer.writeNull(n);
//                        return Done.getInstance();
//                    }).getOrElse(() -> {
//                        writer.writeNull();
//                        return Done.getInstance();
//                    });
//                    return Done.getInstance();
//                }),
//                Case(instanceOf(JsUndefined.class), jsUndefined -> {
//                    writer.writeUndefined();
//                    return Done.getInstance();
//                }),
//                Case(instanceOf(JsNumber.class), jsNumber -> {
//                    name.map(n -> {
//                        writer.writeDecimal128(n, new Decimal128(jsNumber.value));
//                        return Done.getInstance();
//                    }).getOrElse(() -> {
//                        writer.writeDecimal128(new Decimal128(jsNumber.value));
//                        return Done.getInstance();
//                    });
//                    return Done.getInstance();
//                }),
//                Case(instanceOf(JsObject.class), jsObject -> {
//                    name.map(n -> {
//                        if (jsObject.fieldAsOpt("$oid").isDefined()) {
//                            writer.writeObjectId(n, objectId(jsObject));
//                        } else if(jsObject.fieldAsOpt("$date").isDefined()) {
//                            writer.writeDateTime(n, dateTime(jsObject).getTime());
//                        } else {
//                            writer.writeStartDocument(n);
//                            jsObject.values.forEach((n1, value) -> {
//                                encode(Option.some(n1), writer, json, encoderContext);
//                            });
//                            writer.writeEndDocument();
//                        }
//                        return Done.getInstance();
//                    }).getOrElse(() -> {
//                        if (jsObject.fieldAsOpt("$oid").isDefined()) {
//                            writer.writeObjectId(objectId(jsObject));
//                        } else if(jsObject.fieldAsOpt("$date").isDefined()) {
//                            String $date = jsObject.field("$date").asString();
//                            Date date = Try.of(() -> new SimpleDateFormat("").parse($date)).get();
//                            writer.writeDateTime(date.getTime());
//                        } else {
//                            writer.writeStartDocument();
//                            jsObject.values.forEach((n1, value) -> {
//                                encode(Option.some(n1), writer, json, encoderContext);
//                            });
//                            writer.writeEndDocument();
//                        }
//                        return Done.getInstance();
//                    });
//
//                    return Done.getInstance();
//                }),
//                Case(instanceOf(JsString.class), jsString -> {
//                    name.map(n -> {
//                        writer.writeString(n, jsString.value);
//                        return Done.getInstance();
//                    }).getOrElse(() -> {
//                        writer.writeString(jsString.value);
//                        return Done.getInstance();
//                    });
//                    return Done.getInstance();
//                })
//        );
//    }

    @Override
    public Class<JsObject> getEncoderClass() {
        return JsObject.class;
    }
}

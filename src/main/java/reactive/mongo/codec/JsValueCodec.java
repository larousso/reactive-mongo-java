package reactive.mongo.codec;

import akka.Done;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.reactivecouchbase.json.*;
import reactive.mongo.json.MongoReads;

import java.math.BigDecimal;
import java.util.function.Consumer;

import static io.vavr.API.*;
import static io.vavr.Patterns.*;
import static io.vavr.Predicates.*;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsValueCodec implements Codec<JsValue>{

    private final Codec<RawBsonDocument> rawBsonDocumentCodec;
    private final Codec<Document> rawDocumentCodec;

    public JsValueCodec(CodecRegistry codecRegistry) {
        this.rawBsonDocumentCodec = codecRegistry.get(RawBsonDocument.class);
        this.rawDocumentCodec = codecRegistry.get(Document.class);
    }

    @Override
    public JsValue decode(BsonReader reader, DecoderContext decoderContext) {
        Document decode = rawDocumentCodec.decode(reader, decoderContext);
        return Json.parse(decode.toJson());
    }

    @Override
    public void encode(BsonWriter writer, JsValue value, EncoderContext encoderContext) {
        //String stringify = Json.stringify(value);
        //rawDocumentCodec.encode(writer, Document.parse(stringify), encoderContext);
        writeJsValue(Option.none(), value, writer);
    }

    @Override
    public Class<JsValue> getEncoderClass() {
        return JsValue.class;
    }

//"$oid"
//"$date"
//"$regex"
//"$code" => javascript / javascript withScope
//"$timestamp"
//"$minKey"
//"$maxKey"
//"$numberLong"
//"$numberDecimal"
//"$binary"
//"$undefined"
//"$symbol" => deprecated


    private Done writeJsValue(Option<String> name, JsValue jsValue, BsonWriter writer) {
        return Match(jsValue).of(
                Case($(instanceOf(JsString.class)), value -> writeString(name, value, writer)),
                Case($(instanceOf(JsNumber.class)), value -> writeNumber(name, value, writer)),
                Case($(instanceOf(JsBoolean.class)), value -> writeBoolean(name, value, writer)),
                Case($(instanceOf(JsNull.class)), value -> writeNull(name, value, writer)),
                Case($(instanceOf(JsUndefined.class)), value -> writeUndefined(name, value, writer)),
                Case($(instanceOf(JsArray.class)), value -> writeArray(name, value, writer)),
                Case($(instanceOf(JsObject.class)), value -> writeObject(name, value, writer))
        );
    }

    private Done writeArray(Option<String> name, JsArray value, BsonWriter writer) {
        ifElse(name,
                n -> writer.writeStartArray(n),
                () -> writer.writeStartArray()
        );
        value.forEach(jsValue -> writeJsValue(Option.none(), jsValue, writer));
        writer.writeEndArray();
        return Done.getInstance();
    }


    private Done writeUndefined(Option<String> name, JsUndefined value, BsonWriter writer) {
        ifElse(name,
                n -> writer.writeUndefined(n),
                () -> writer.writeUndefined()
        );
        return Done.getInstance();
    }

    private Done writeNull(Option<String> name, JsNull value, BsonWriter writer) {
        ifElse(name,
                n -> writer.writeNull(n),
                () -> writer.writeNull()
        );
        return Done.getInstance();
    }

    private Done writeBoolean(Option<String> name, JsBoolean value, BsonWriter writer) {
        ifElse(name,
            n -> writer.writeBoolean(n, value.value),
            () -> writer.writeBoolean(value.value)
        );
        return Done.getInstance();
    }

    private Done writeString(Option<String> name, JsString jsString, BsonWriter writer) {
        ifElse(name,
                n -> writer.writeString(n, jsString.value),
                () -> writer.writeString(jsString.value)
        );
        return Done.getInstance();
    }

    private Done writeNumber(Option<String> name, JsNumber jsNumber, BsonWriter writer) {
        if(jsNumber.value.stripTrailingZeros().scale() <= 0) {
            ifElse(name,
                    n -> writer.writeInt64(n, jsNumber.value.intValue()),
                    () -> writer.writeInt64(jsNumber.value.intValue())
            );
        } else {
            ifElse(name,
                    n -> writer.writeDecimal128(n, new Decimal128(jsNumber.value)),
                    () -> writer.writeDecimal128(new Decimal128(jsNumber.value))
            );
        }
        return Done.getInstance();
    }

    private Done writeObject(Option<String> name, JsObject jsObject, BsonWriter writer) {
        if (jsObject.fieldAsOpt("$oid").isDefined()) {
            ObjectId oid = jsObject.as(MongoReads.objectId);
            ifElse(name,
                n -> writer.writeObjectId(n, oid),
                () -> writer.writeObjectId(oid)
            );
        } else if (jsObject.fieldAsOpt("$date").isDefined()) {
            long time = jsObject.as(MongoReads.date).getTime();
            ifElse(name,
                    n -> writer.writeDateTime(n, time),
                    () -> writer.writeDateTime(time)
            );
        } else if (jsObject.fieldAsOpt("$timestamp").isDefined()) {
            Tuple2<Integer, Integer> ts = jsObject.as(MongoReads.timestamp);
            ifElse(name,
                    n -> writer.writeTimestamp(n, new BsonTimestamp(ts._1, ts._2)),
                    () -> writer.writeTimestamp(new BsonTimestamp(ts._1, ts._2))
            );
        } else if (jsObject.fieldAsOpt("$minKey").isDefined()) {
            ifElse(name,
                    writer::writeMinKey,
                    writer::writeMinKey
            );
        } else if (jsObject.fieldAsOpt("$maxKey").isDefined()) {
            ifElse(name,
                    writer::writeMaxKey,
                    writer::writeMaxKey
            );
        } else if (jsObject.fieldAsOpt("$numberLong").isDefined()) {
            Long numberLong = jsObject.as(MongoReads.numberLong);
            ifElse(name,
                n -> writer.writeInt64(n, numberLong),
                () -> writer.writeInt64(numberLong)
            );
        } else if (jsObject.fieldAsOpt("$numberDecimal").isDefined()) {
            BigDecimal numberDecimal = jsObject.as(MongoReads.numberDecimal);
            ifElse(name,
                n -> writer.writeDecimal128(n, new Decimal128(numberDecimal)),
                () -> writer.writeDecimal128(new Decimal128(numberDecimal))
            );
        } else if (jsObject.fieldAsOpt("$binary").isDefined()) {
            Tuple2<Byte, byte[]> as = jsObject.as(MongoReads.binary);
            ifElse(name,
                    n -> writer.writeBinaryData(n, new BsonBinary(as._1, as._2)),
                    () -> writer.writeBinaryData(new BsonBinary(as._1, as._2))
            );
        } else if (jsObject.fieldAsOpt("$undefined").isDefined()) {
            ifElse(name, n -> writer.writeUndefined(n), () -> writer.writeUndefined());
        } else if (jsObject.fieldAsOpt("$code").isDefined()) {
            if (jsObject.fieldAsOpt("$scope").isDefined()) {
                throw new RuntimeException("not impleted yet");
            } else {
                ifElse(name,
                        n -> writer.writeJavaScript(n, jsObject.field("$code").asString()),
                        () -> writer.writeJavaScript(jsObject.field("$code").asString())
                );
            }
        } else if (jsObject.fieldAsOpt("$symbol").isDefined()) {
            String symbol = jsObject.as(MongoReads.symbol);
            ifElse(name,
                    n -> writer.writeSymbol(n, symbol),
                    () -> writer.writeSymbol(symbol)
            );
        } else {
            ifElse(name, writer::writeStartDocument, writer::writeStartDocument);
            jsObject.values.forEach(p -> {
                writeJsValue(Option.of(p._1), p._2, writer);
            });
            writer.writeEndDocument();
        }
        return Done.getInstance();
    }

    public <T> void ifElse(Option<T> opt, Consumer<T> c, Consumer0 c0) {
        opt.forEach(c);
        if(opt.isEmpty()) {
            c0.apply();
        }

    }


    interface Consumer0 {
        void apply();
    }

}

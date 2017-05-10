package reactive.mongo.codec.tmp;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.reactivecouchbase.json.JsNumber;

import static javaslang.API.*;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsNumberCodec implements Codec<JsNumber> {
    @Override
    public JsNumber decode(BsonReader reader, DecoderContext decoderContext) {
        BsonType currentBsonType = reader.getCurrentBsonType();
        return Match(currentBsonType).of(
                Case($(BsonType.DOUBLE), any -> new JsNumber(reader.readDouble())),
                Case($(BsonType.DECIMAL128), any -> new JsNumber(reader.readDecimal128().bigDecimalValue())),
                Case($(BsonType.INT32), any -> new JsNumber(reader.readInt32())),
                Case($(BsonType.INT64), any -> new JsNumber(reader.readInt64()))
        );
    }

    @Override
    public void encode(BsonWriter writer, JsNumber value, EncoderContext encoderContext) {

    }

    @Override
    public Class<JsNumber> getEncoderClass() {
        return null;
    }
}

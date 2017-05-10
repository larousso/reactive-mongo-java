package reactive.mongo.codec.tmp;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.reactivecouchbase.json.JsNull;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsNullCodec implements Codec<JsNull> {
    @Override
    public JsNull decode(BsonReader reader, DecoderContext decoderContext) {
        return null;
    }

    @Override
    public void encode(BsonWriter writer, JsNull value, EncoderContext encoderContext) {

    }

    @Override
    public Class<JsNull> getEncoderClass() {
        return JsNull.class;
    }
}

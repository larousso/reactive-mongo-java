package reactive.mongo.codec.tmp;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.reactivecouchbase.json.JsUndefined;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsUndefinedCodec implements Codec<JsUndefined> {
    @Override
    public JsUndefined decode(BsonReader reader, DecoderContext decoderContext) {
        return null;
    }

    @Override
    public void encode(BsonWriter writer, JsUndefined value, EncoderContext encoderContext) {

    }

    @Override
    public Class<JsUndefined> getEncoderClass() {
        return null;
    }
}

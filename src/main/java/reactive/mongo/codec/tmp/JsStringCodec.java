package reactive.mongo.codec.tmp;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.reactivecouchbase.json.JsString;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsStringCodec implements Codec<JsString> {
    @Override
    public JsString decode(BsonReader reader, DecoderContext decoderContext) {
        return null;
    }

    @Override
    public void encode(BsonWriter writer, JsString value, EncoderContext encoderContext) {

    }

    @Override
    public Class<JsString> getEncoderClass() {
        return null;
    }
}

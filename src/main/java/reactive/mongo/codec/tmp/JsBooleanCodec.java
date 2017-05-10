package reactive.mongo.codec.tmp;

import org.bson.BsonBoolean;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.reactivecouchbase.json.JsBoolean;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsBooleanCodec implements Codec<JsBoolean> {

    @Override
    public JsBoolean decode(BsonReader reader, DecoderContext decoderContext) {
        boolean value = reader.readBoolean();
        return new JsBoolean(value);
    }

    @Override
    public void encode(BsonWriter writer, JsBoolean value, EncoderContext encoderContext) {
        writer.writeBoolean(value.value);
    }

    @Override
    public Class<JsBoolean> getEncoderClass() {
        return JsBoolean.class;
    }
}

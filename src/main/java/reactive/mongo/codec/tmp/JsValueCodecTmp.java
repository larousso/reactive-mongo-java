package reactive.mongo.codec.tmp;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivecouchbase.json.JsValue;
import reactive.mongo.codec.tmp.JsObjectCodecProvider;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsValueCodecTmp implements Codec<JsValue> {

    private final CodecRegistry codecRegistry;

    /**
     * Creates a new instance with a default codec registry that uses the {@link BsonValueCodecProvider}.
     */
    public JsValueCodecTmp() {
        this(fromProviders(new BsonValueCodecProvider()));
    }

    /**
     * Creates a new instance initialised with the given codec registry.
     *
     * @param codecRegistry the {@code CodecRegistry} to use to look up the codecs for encoding and decoding to/from BSON
     */
    public JsValueCodecTmp(final CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public JsValue decode(final BsonReader reader, final DecoderContext decoderContext) {
        return codecRegistry.get(JsObjectCodecProvider.getClassForBsonType(reader.getCurrentBsonType())).decode(reader, decoderContext);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void encode(final BsonWriter writer, final JsValue value, final EncoderContext encoderContext) {
        Codec codec = codecRegistry.get(value.getClass());
        encoderContext.encodeWithChildContext(codec, writer, value);
    }

    @Override
    public Class<JsValue> getEncoderClass() {
        return JsValue.class;
    }
}

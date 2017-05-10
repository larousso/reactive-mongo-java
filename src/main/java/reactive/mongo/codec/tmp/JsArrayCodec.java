package reactive.mongo.codec.tmp;

import org.bson.*;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivecouchbase.json.JsArray;
import org.reactivecouchbase.json.JsValue;

import java.util.ArrayList;
import java.util.List;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsArrayCodec implements Codec<JsArray> {

    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(new JsObjectCodecProvider());

    private final CodecRegistry codecRegistry;

    /**
     * Creates a new instance with a default codec registry that uses the {@link BsonValueCodecProvider}.
     *
     * @since 3.4
     */
    public JsArrayCodec() {
        this(DEFAULT_REGISTRY);
    }

    /**
     * Construct an instance with the given registry
     *
     * @param codecRegistry the codec registry
     */
    public JsArrayCodec(final CodecRegistry codecRegistry) {
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
    }

    @Override
    public JsArray decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        List<JsValue> list = new ArrayList<JsValue>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext));
        }
        reader.readEndArray();

        return new JsArray(javaslang.collection.List.ofAll(list));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void encode(final BsonWriter writer, final JsArray array, final EncoderContext encoderContext) {
        writer.writeStartArray();

        for (JsValue value : array) {
            Codec codec = codecRegistry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, writer, value);
        }

        writer.writeEndArray();
    }

    @Override
    public Class<JsArray> getEncoderClass() {
        return JsArray.class;
    }

    /**
     * This method may be overridden to change the behavior of reading the current value from the given {@code BsonReader}.  It is required
     * that the value be fully consumed before returning.
     *
     * @param reader the read to read the value from
     * @param decoderContext the decoder context
     * @return the non-null value read from the reader
     */
    protected JsValue readValue(final BsonReader reader, final DecoderContext decoderContext) {
        return codecRegistry.get(JsObjectCodecProvider.getClassForBsonType(reader.getCurrentBsonType())).decode(reader, decoderContext);
    }
}

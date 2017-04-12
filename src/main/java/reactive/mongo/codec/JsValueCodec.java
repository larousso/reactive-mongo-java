package reactive.mongo.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.reactivecouchbase.json.JsValue;

/**
 * Created by 97306p on 12/04/2017.
 */
public class JsValueCodec implements Codec<JsValue> {

    private final Codec<Document> documentCodec;
    private final Conversions conversions;

    public JsValueCodec(Conversions conversions) {
        this.conversions = conversions;
        documentCodec = conversions.codecRegistry.get(Document.class);
    }

    @Override
    public JsValue decode(BsonReader reader, DecoderContext decoderContext) {
        Document document = this.documentCodec.decode(reader, decoderContext);
        return conversions.fromDocument(document);
    }

    @Override
    public void encode(BsonWriter writer, JsValue value, EncoderContext encoderContext) {
        Document document = conversions.toDocument(value);
        this.documentCodec.encode(writer, document, encoderContext);
    }

    @Override
    public Class<JsValue> getEncoderClass() {
        return JsValue.class;
    }
}

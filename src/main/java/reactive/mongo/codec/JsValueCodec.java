package reactive.mongo.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import javax.print.Doc;

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
        String stringify = Json.stringify(value);
        rawDocumentCodec.encode(writer, Document.parse(stringify), encoderContext);
    }

    @Override
    public Class<JsValue> getEncoderClass() {
        return JsValue.class;
    }
}

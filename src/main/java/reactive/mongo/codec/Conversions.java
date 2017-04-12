package reactive.mongo.codec;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by adelegue on 12/04/2017.
 */
public class Conversions {

    final ObjectMapper mapper;
    final CodecRegistry codecRegistry;

    public Conversions(ObjectMapper mapper, CodecRegistry codecRegistry) {
        this.mapper = mapper;
        this.codecRegistry = codecRegistry;
    }

    public JsValue fromDocument(Document document) {
        return Json.fromJsonNode(mapper.convertValue(document, JsonNode.class));
    }

    public Document toDocument(JsValue json) {
        return mapper.convertValue(Json.toJackson(json), Document.class);
    }

    public BsonDocument toBson(JsValue json) {
        Document document = toDocument(json);
        return document.toBsonDocument(BsonDocument.class, codecRegistry);
    }
}

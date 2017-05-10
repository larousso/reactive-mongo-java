package reactive.mongo.codec.tmp;

import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Predicates.instanceOf;

import javaslang.control.Option;
import org.bson.*;
import org.bson.types.Decimal128;
import org.reactivecouchbase.json.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by adelegue on 12/04/2017.
 */
public class Conversions {

    final ObjectMapper mapper;

    public Conversions(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public JsValue fromDocument(Document document) {
        return Json.fromJsonNode(mapper.convertValue(document, JsonNode.class));
    }

    public Document toDocument(JsValue json) {
        return mapper.convertValue(Json.toJackson(json), Document.class);
    }

    public BsonDocument toBson(JsObject json) {
        return toBsonDocument(json).asDocument();
    }

    private BsonValue toBsonArray(JsArray array) {
        return new BsonArray(array.values.map(e -> this.toBsonValue(Option.none(), e)).toJavaList());
    }

    private BsonValue toBsonBoolean(JsBoolean bool) {
        return new BsonBoolean(bool.value);
    }

    private BsonValue toBsonNull(JsValue value) {
        return new BsonNull();
    }

    private BsonValue toBsonNumber(JsNumber value) {
        return new BsonDecimal128(new Decimal128(value.value));
    }

    private BsonValue toBsonDocument(JsObject obj) {
        BsonDocument bsonDocument = new BsonDocument();
        obj.values.forEach(pair -> bsonDocument.append(pair._1, toBsonValue(Option.some(pair._1), pair._2)));
        return bsonDocument;
    }

    private BsonValue toBsonString(JsString str) {
        return new BsonString(str.value);
    }

    public BsonValue toBsonValue(Option<String> fieldName, JsValue json) {
        return Match(json).of(
                Case(instanceOf(JsArray.class), this::toBsonArray),
                Case(instanceOf(JsBoolean.class), this::toBsonBoolean),
                Case(instanceOf(JsNull.class), this::toBsonNull),
                Case(instanceOf(JsUndefined.class), this::toBsonNull),
                Case(instanceOf(JsNumber.class), this::toBsonNumber),
                Case(instanceOf(JsObject.class), this::toBsonDocument),
                Case(instanceOf(JsString.class), this::toBsonString)
        );
    }
}

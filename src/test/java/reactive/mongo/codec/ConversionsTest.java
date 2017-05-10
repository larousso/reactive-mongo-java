package reactive.mongo.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.reactivecouchbase.json.Syntax.$;

import java.math.BigInteger;
import java.util.LinkedHashMap;

import org.bson.*;
import org.junit.Test;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import com.fasterxml.jackson.databind.ObjectMapper;
import reactive.mongo.codec.tmp.Conversions;

/**
 * Created by 97306p on 12/04/2017.
 */
public class ConversionsTest {

    @Test
    public void fromDocument() {
        Document document = new Document()
                .append("number", 1)
                .append("string", "string")
                .append("obj", new Document().append("obj", "obj"))
                .append("null", null);
        JsObject json = Json.obj(
                $("number", 1),
                $("string", "string"),
                $("obj", $("obj", "obj")),
                $("null", new JsNull())
        );
        Conversions conversions = new Conversions(new ObjectMapper());

        JsValue converted = conversions.fromDocument(document);
        assertThat(converted).isEqualTo(json);
    }

    @Test
    public void toDocument() {
        Document document = new Document()
                .append("number", 1)
                .append("string", "string")
                .append("obj", new Document().append("obj", "obj"))
                .append("null", null);
        JsObject json = Json.obj(
                $("number", 1),
                $("string", "string"),
                $("obj", $("obj", "obj")),
                $("null", new JsNull())
        );
        Conversions conversions = new Conversions(new ObjectMapper());

        Document converted = conversions.toDocument(json);

        assertThat(converted.get("number", BigInteger.class).intValue()).isEqualTo(document.get("number", Integer.class));
        assertThat(converted.get("string", String.class)).isEqualTo(document.get("string", String.class));
        assertThat(converted.get("null")).isEqualTo(document.get("null"));
        assertThat(converted.get("obj", LinkedHashMap.class).get("obj")).isEqualTo(document.get("obj", Document.class).get("obj"));
    }

    @Test
    public void toBson() {
        BsonDocument document = new BsonDocument()
                .append("number", new BsonInt32(1))
                .append("string", new BsonString("string"))
                .append("obj",  new BsonDocument().append("obj", new BsonString("obj")))
                .append("null", new BsonNull());

        JsObject json = Json.obj(
                $("number", 1),
                $("string", "string"),
                $("obj", $("obj", "obj")),
                $("null", new JsNull())
        );
        Conversions conversions = new Conversions(new ObjectMapper());

        BsonDocument converted = conversions.toBson(json);

        assertThat(converted.get("number").asDecimal128().intValue()).isEqualTo(document.get("number").asInt32().intValue());
        assertThat(converted.getString("string")).isEqualTo(document.getString("string"));
        assertThat(converted.get("null")).isEqualTo(document.get("null"));
        assertThat(converted.getDocument("obj").getString("obj")).isEqualTo(document.getDocument("obj").getString("obj"));
    }

}
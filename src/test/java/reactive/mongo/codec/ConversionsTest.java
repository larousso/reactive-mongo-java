package reactive.mongo.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.async.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.junit.Test;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import java.math.BigInteger;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.*;
import static org.reactivecouchbase.json.Syntax.*;
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
        Conversions conversions = new Conversions(new ObjectMapper(), null);

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
        Conversions conversions = new Conversions(new ObjectMapper(), null);

        Document converted = conversions.toDocument(json);

        assertThat(converted.get("number", BigInteger.class).intValue()).isEqualTo(document.get("number", Integer.class));
        assertThat(converted.get("string", String.class)).isEqualTo(document.get("string", String.class));
        assertThat(converted.get("null")).isEqualTo(document.get("null"));
        assertThat(converted.get("obj", LinkedHashMap.class).get("obj")).isEqualTo(document.get("obj", Document.class).get("obj"));
    }

}
package reactive.mongo.conversion;

import org.bson.Document;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

/**
 * Created by adelegue on 12/04/2017.
 */
public class Conversions {

    public static JsValue fromDocument(Document document) {
        return Json.toJson(document);
    }

    public static Document toDocument(JsValue json) {
        return new Document();
    }

}

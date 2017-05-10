package reactive.mongo.json;

import org.bson.types.ObjectId;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Writer;

import java.util.Date;
import static org.reactivecouchbase.json.Syntax.*;

/**
 * Created by adelegue on 10/05/2017.
 */
public class MongoWrites {

    public static Writer<Date> dateWrites = d -> {
        return Json.obj($("$date", d.getTime()));
    };

    public static JsObject date(Date date) {
        return Json.toJson(date, dateWrites).asObject();
    }

    public static Writer<ObjectId> objectIdWrites = oid -> {
        return Json.obj($("iod", oid.toString()));
    };
}

package reactive.mongo.json;

import javaslang.control.Option;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsString;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Writer;

import java.math.BigDecimal;
import java.util.Date;
import static org.reactivecouchbase.json.Syntax.*;

/**
 * Created by adelegue on 10/05/2017.
 */
public class MongoWrites {

//"$date"
//"$oid"
//"$regex"
//"$code" => javascript / javascript withScope
//"$timestamp"
//"$numberLong"
//"$undefined"
//"$binary"
//"$maxKey"
//"$minKey"
//"$numberDecimal"
//"$symbol" => deprecated

    public static Writer<Date> date = d -> Json.obj($("$date", d.getTime()));

    public static JsObject date(Date date) {
        return Json.toJson(date, MongoWrites.date).asObject();
    }

    public static Writer<ObjectId> objectId = oid -> Json.obj($("$oid", oid.toString()));

    public static JsObject objectId(ObjectId id) {
        return Json.toJson(id, MongoWrites.objectId).asObject();
    }

    public static JsObject objectId(String id) {
        return Json.toJson(new ObjectId(id), MongoWrites.objectId).asObject();
    }

    public static Writer<BsonRegularExpression> regexp = d -> Option.of(d.getOptions())
            .map(o -> Json.obj($("$regex", d.getPattern()), $("$options", o)))
            .getOrElse(Json.obj($("$regex",d.getPattern())));

    public static JsObject regex(String pattern) {
        return regex(pattern, null);
    }

    public static JsObject regex(String pattern, String option) {
        BsonRegularExpression bsonRegularExpression = new BsonRegularExpression(pattern, option);
        return Json.toJson(bsonRegularExpression, MongoWrites.regexp).asObject();
    }

    public static Writer<String> type = t -> Json.obj($("$type", t));

    public static JsObject type(String t) {
        return Json.toJson(t, MongoWrites.type).asObject();
    }


    public static Writer<BsonJavaScript> javascript = t -> Json.obj($("$code", t.getCode()));

    public static JsObject javascript(String code) {
        return Json.toJson(new BsonJavaScript(code), MongoWrites.javascript).asObject();
    }


    public static JsObject javascriptWithScope(String code, JsObject scope) {
        return Json.obj(
                $("$code", code),
                $("$scope", scope)
        );
    }


    public static Writer<BsonTimestamp> timestamp = t -> Json.obj(
            $("t", t.getTime()),
            $("i", t.getInc())
    );

    public static JsObject timestamp(Integer seconds, Integer inc) {
        return Json.toJson(new BsonTimestamp(seconds, inc), MongoWrites.timestamp).asObject();
    }

    public static Writer<BsonInt64> numberLong = l -> Json.obj($("$numberLong", l.getValue()));

    public static JsObject numberLong(Long l) {
        return Json.toJson(new BsonInt64(l), MongoWrites.numberLong).asObject();
    }

    public static Writer<BsonDecimal128> numberDecimal = l -> Json.obj($("$numberDecimal", l.toString()));

    public static JsObject numberDecimal(BigDecimal bigDecimal) {
        return Json.toJson(new BsonDecimal128(new Decimal128(bigDecimal)), MongoWrites.numberDecimal).asObject();
    }

    public static Writer<BsonUndefined> undefined = u -> Json.obj($("$undefined", true));

    public static JsObject undefined() {
        return Json.toJson(new BsonUndefined(), MongoWrites.undefined).asObject();
    }

    public static Writer<BsonMaxKey> maxKey = u -> Json.obj($("$maxKey", 1));

    public static JsObject maxKey() {
        return Json.toJson(new BsonMaxKey(), MongoWrites.maxKey).asObject();
    }

    public static Writer<BsonMinKey> minKey = u -> Json.obj($("$minKey", 1));

    public static JsObject minKey() {
        return Json.toJson(new BsonMinKey(), MongoWrites.minKey).asObject();
    }

    public static Writer<BsonBinary> binary = b -> Json.obj(
            $("$binary", new String(b.getData())),
            $("$type", Byte.toString(b.getType()))
    );

    public static JsObject binary(byte[] binData, byte type) {
        return Json.toJson(new BsonBinary(type, binData), MongoWrites.binary).asObject();
    }

}

package reactive.mongo.json;

import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.control.Option;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.reactivecouchbase.json.*;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;

import javax.xml.bind.DatatypeConverter;
import java.math.BigDecimal;
import java.util.Date;
import java.util.function.Function;

import static javaslang.API.*;
import static javaslang.Predicates.*;

/**
 * Created by adelegue on 10/05/2017.
 */
public class MongoReads {

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

    private static <T> Reader<T> field(String f, Function<JsValue, Option<T>> fun) {
        return json -> {
            if (json.fieldAsOpt(f).isDefined()) {
                return fun.apply(json.field(f))
                        .map(JsResult::success)
                        .getOrElse(JsResult.error(new IllegalStateException("Wrong format")));
            } else {
                return JsResult.error(new IllegalStateException("Wrong structure, " + f + " field expected"));
            }
        };
    }

    public static Reader<Date> date =
        field("$date", $date ->
                Match($date).option(
                    Case(instanceOf(JsNumber.class), n -> new Date(n.value.longValue())),
                    Case(instanceOf(JsString.class), n -> new Date(DatatypeConverter.parseDateTime(n.value).getTimeInMillis()))
                )
        );

    public static Reader<BsonDateTime> bsonDateTime =
            field("$date", $date ->
                    Match($date).option(
                            Case(instanceOf(JsNumber.class), n -> new BsonDateTime(n.value.longValue())),
                            Case(instanceOf(JsString.class), n -> new BsonDateTime(DatatypeConverter.parseDateTime(n.value).getTimeInMillis()))
                    )
            );

    public static Reader<ObjectId> objectId = field("$oid", $oid ->
            Match($oid).option(
                    Case(instanceOf(JsString.class), n -> new ObjectId(n.value))
            )
    );

    public static Reader<BsonObjectId> bsonObjectId = json -> objectId.read(json).map(BsonObjectId::new);

    public static Reader<String> strObjectId = field("$oid", $oid ->
            Match($oid).option(
                    Case(instanceOf(JsString.class), n -> n.value)
            )
    );

    public static Reader<Long> numberLong = field("$numberLong", $numberLong ->
            Match($numberLong).option(
                    Case(instanceOf(JsNumber.class), n -> n.value.longValue()),
                    Case(instanceOf(JsString.class), n -> Long.decode(n.value))
            )
    );

    public static Reader<BsonInt64> bsonNumberLong = field("$numberLong", $numberLong ->
            Match($numberLong).option(
                    Case(instanceOf(JsNumber.class), n -> new BsonInt64(n.value.longValue())),
                    Case(instanceOf(JsString.class), n -> new BsonInt64(Long.decode(n.value)))
            )
    );

    public static Reader<BigDecimal> numberDecimal = field("$numberDecimal", $numberDecimal ->
            Match($numberDecimal).option(
                    Case(instanceOf(JsNumber.class), n -> n.value)
            )
    );

    public static Reader<BsonDecimal128> bsonNumberDecimal = field("$numberDecimal", $numberDecimal ->
            Match($numberDecimal).option(
                    Case(instanceOf(JsNumber.class), n -> new BsonDecimal128(new Decimal128(n.value)))
            )
    );

    public static Reader<BsonBinary> bsonBinary = json -> {
        if(json.fieldAsOpt("$binary").isDefined() && json.fieldAsOpt("$type").isDefined()) {
            return JsResult.success(new BsonBinary(Byte.parseByte(json.field("$type").asString()), json.field("$binary").asString().getBytes()));
        } else {
            return JsResult.error(new IllegalStateException("Wrong structure, $binary and $type field expected"));
        }
    };

    public static Reader<Tuple2<Byte, byte[]>> binary = json ->
        bsonBinary.read(json).map(b -> Tuple.of(b.getType(), b.getData()));

    public static Reader<String> symbol = field("$symbol", $symbol ->
            Match($symbol).option(
                    Case(instanceOf(JsString.class), n -> n.value)
            )
    );

    public static Reader<BsonSymbol> bsonSymbol = field("$symbol", $symbol ->
            Match($symbol).option(
                    Case(instanceOf(JsString.class), n -> new BsonSymbol(n.value))
            )
    );

    public static Reader<Tuple2<Integer, Integer>> timestamp = field("$timestamp", $timestamp ->
            Match($timestamp).option(
                    Case(instanceOf(JsObject.class), n ->
                        Tuple.of(n.field("i").asInteger(), n.field("i").asInteger())
                    )
            )
    );

    public static Reader<BsonTimestamp> bsonTimestamp = field("$timestamp", $timestamp ->
            Match($timestamp).option(
                    Case(instanceOf(JsObject.class), n ->
                            new BsonTimestamp(n.field("i").asInteger(), n.field("i").asInteger())
                    )
            )
    );


    public static Reader<BsonUndefined> bsonUndefinedFromObject = field("$undefined", $undefined ->
            Match($undefined).option(
                    Case($(), n -> new BsonUndefined())
            )
    );


    public static Reader<BsonValue> bsonJavascript = json -> {
        if(json.fieldAsOpt("$code").isDefined()) {
            String $code = json.field("$code").asString();
            return JsResult.success(json.fieldAsOpt("$scope").<BsonValue>map(scope ->
                new BsonJavaScriptWithScope($code, json.field("$scope").as(MongoReads.bsonDocument))
            ).getOrElse(() -> new BsonJavaScript($code)));
        } else {
            return JsResult.error(new IllegalStateException("Wrong structure, $code field expected"));
        }
     };

    public static Reader<BsonDocument> bsonDocument = json -> Match(json).option(
            Case(instanceOf(JsObject.class), jsObject -> new BsonDocument(jsObject.values.map(p ->
                    new BsonElement(p._1, p._2.as(MongoReads.bsonValue))
            ).toJavaList()))
        )
        .map(JsResult::success)
        .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, object expected")));

    public static Reader<BsonValue> objToBsonValue = json -> Match(json).option(
            Case(instanceOf(JsObject.class), jsObject -> {
                if (jsObject.fieldAsOpt("$oid").isDefined()) {
                    return jsObject.as(MongoReads.bsonObjectId);
                } else if (jsObject.fieldAsOpt("$date").isDefined()) {
                    return jsObject.as(MongoReads.bsonDateTime);
                } else if (jsObject.fieldAsOpt("$timestamp").isDefined()) {
                    return jsObject.as(MongoReads.bsonTimestamp);
                } else if (jsObject.fieldAsOpt("$minKey").isDefined()) {
                    return new BsonMinKey();
                } else if (jsObject.fieldAsOpt("$maxKey").isDefined()) {
                    return new BsonMaxKey();
                } else if (jsObject.fieldAsOpt("$numberLong").isDefined()) {
                    return jsObject.as(MongoReads.bsonNumberLong);
                } else if (jsObject.fieldAsOpt("$numberDecimal").isDefined()) {
                    return jsObject.as(MongoReads.bsonNumberDecimal);
                } else if (jsObject.fieldAsOpt("$binary").isDefined()) {
                    return jsObject.as(MongoReads.bsonBinary);
                } else if (jsObject.fieldAsOpt("$undefined").isDefined()) {
                    return jsObject.as(MongoReads.bsonUndefinedFromObject);
                } else if (jsObject.fieldAsOpt("$code").isDefined()) {
                    return jsObject.as(MongoReads.bsonJavascript);
                } else if (jsObject.fieldAsOpt("$symbol").isDefined()) {
                    return jsObject.as(MongoReads.bsonSymbol);
                } else {
                    return new BsonDocument(jsObject.values.map(p ->
                            new BsonElement(p._1, p._2.as(MongoReads.bsonValue))
                    ).toJavaList());
                }
            })
    )
    .map(JsResult::success)
    .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, object expected")));

    public static Reader<BsonBoolean> bsonBoolean = json -> Match(json).option(
            Case(instanceOf(JsBoolean.class), bool -> new BsonBoolean(bool.value))
        )
        .map(JsResult::success)
        .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, boolean expected")));

    public static Reader<BsonNull> bsonNull = json -> Match(json).option(
            Case(instanceOf(JsNull.class), any -> new BsonNull())
        )
        .map(JsResult::success)
        .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, null expected")));

    public static Reader<BsonUndefined> bsonUndefined = json -> Match(json).option(
            Case(instanceOf(JsUndefined.class), any -> new BsonUndefined())
        )
        .map(JsResult::success)
        .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, undefined expected")));


    public static Reader<BsonNumber> bsonNumber = json -> Match(json).option(
            Case(instanceOf(JsNumber.class), number -> {
                if(number.value.stripTrailingZeros().scale() <= 0) {
                    return new BsonInt64(number.value.intValue());
                } else {
                    return new BsonDecimal128(new Decimal128(number.value));
                }
            }))
            .map(JsResult::success)
            .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, number expected")));

    public static Reader<BsonString> bsonString = json -> Match(json).option(
            Case(instanceOf(JsString.class), str -> new BsonString(str.value)))
            .map(JsResult::success)
            .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, string expected")));

    public static Reader<BsonArray> bsonArray = json -> Match(json).option(
                Case(instanceOf(JsArray.class), arr ->
                    new BsonArray(arr.values.map(j -> j.as(MongoReads.bsonValue)).toJavaList()))
            )
            .map(JsResult::success)
            .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure, array expected")));



    public static Reader<BsonValue> bsonValue = json -> Match(json).option(
            Case(instanceOf(JsArray.class), a -> json.as(MongoReads.bsonArray)),
            Case(instanceOf(JsBoolean.class), b -> json.as(MongoReads.bsonBoolean)),
            Case(instanceOf(JsNull.class), n -> json.as(MongoReads.bsonNull)),
            Case(instanceOf(JsUndefined.class), n -> json.as(MongoReads.bsonUndefined)),
            Case(instanceOf(JsNumber.class), n -> json.as(MongoReads.bsonNumber)),
            Case(instanceOf(JsObject.class), o -> json.as(MongoReads.objToBsonValue)),
            Case(instanceOf(JsString.class), s -> json.as(MongoReads.bsonString))
        )
        .map(JsResult::success)
        .getOrElse(() -> JsResult.error(new IllegalStateException("Wrong structure")));

}


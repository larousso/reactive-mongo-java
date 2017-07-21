package reactive.mongo;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.mongodb.ServerAddress;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.reactivestreams.client.Success;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;
import org.reactivecouchbase.json.mapping.Writer;
import reactive.mongo.json.MongoReads;
import reactive.mongo.json.MongoWrites;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static com.mongodb.client.model.Filters.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.reactivecouchbase.json.Syntax.$;
import static reactive.mongo.reads.JsValueReads.reader;

/**
 * Created by adelegue on 12/04/2017.
 */
public class ReactiveMongoClientTest implements WithMongo {

    private Random random = new Random();

    private String databaseName;
    private MongoDatabase database;
    private ReactiveMongoClient client;
    private ActorSystem actorSystem;

    @BeforeClass
    public static void setUpMongoDb() throws IOException {
        //WithMongo.startMongo();
    }

    @Before
    public void setUp() throws ExecutionException, InterruptedException {
        actorSystem = ActorSystem.create();
        //Integer port = port();
        Integer port = 27017;
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .hosts(Collections.singletonList(new ServerAddress("localhost", port)))
                .build();
        MongoClientSettings settings = MongoClientSettings.builder().clusterSettings(clusterSettings).build();
        client = ReactiveMongoClient.create(actorSystem, settings);
        databaseName = "test-" + random.nextInt();
        database = client.getDatabase(databaseName);
        database.drop().one().toCompletableFuture().get();

    }

    @After
    public void cleanUp() throws ExecutionException, InterruptedException {
        database.drop().one().toCompletableFuture().get();
    }

    @Test
    public void json_search_by_date() throws ExecutionException, InterruptedException, ParseException {
        MongoCollection<JsValue> collection = database.getJsonCollection("json-vikings");

        Date created = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01");
        ObjectId id = ObjectId.get();
        JsObject ragnar = Json.obj(
                $("_id", MongoWrites.objectId(id)),
                $("name", "Ragnard"),
                $("created", MongoWrites.date(created)),
                $("childs", Json.arr($("name", "Bjorn")))
        );

        JsObject floki = Json.obj($("_id", MongoWrites.objectId(ObjectId.get())), $("name", "Floki"), $("created", MongoWrites.date(new Date())));
        JsObject rollo = Json.obj($("_id", MongoWrites.objectId(ObjectId.get())), $("name", "Rollo"), $("created", MongoWrites.date(new Date())));

        collection.insertMany(Arrays.asList(ragnar, floki, rollo)).one().toCompletableFuture().get();

        Date lowerDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01");
        Date higherDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-02");
        Bson created1 = and(gte("created", lowerDate), lte("created", higherDate));
        System.out.println(created1);

        JsObject query = Json.obj($("created", Json.obj(
                $("$gte", MongoWrites.date(lowerDate)),
                $("$lte", MongoWrites.date(higherDate))
        )));

        List<JsValue> searchByDate = collection.find(query)
                .list().toCompletableFuture().get();

        assertThat(searchByDate).isNotEmpty();
        assertThat(searchByDate).hasSize(1);
        assertThat(searchByDate).containsExactly(ragnar);
    }


    @Test
    public void json_search_by_id() throws ExecutionException, InterruptedException, ParseException {
        MongoCollection<JsValue> collection = database.getJsonCollection("json-vikings");

        Date created = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01");
        ObjectId id = ObjectId.get();
        JsObject ragnar = Json.obj(
                $("_id", MongoWrites.objectId(id)),
                $("name", "Ragnard"),
                $("created", MongoWrites.date(created)),
                $("childs", Json.arr($("name", "Bjorn")))
        );
        collection.insertOne(ragnar).one().toCompletableFuture().get();

        JsObject query = Json.obj($("_id", MongoWrites.objectId(id)));

        Option<JsValue> searchByDate = collection.find(query)
                .one().toCompletableFuture().get();

        assertThat(searchByDate).isNotEmpty();
        assertThat(searchByDate.get()).isEqualTo(ragnar);
    }


    @Test
    public void json_search_by_regex() throws ExecutionException, InterruptedException, ParseException {
        MongoCollection<JsValue> collection = database.getJsonCollection("json-vikings");

        Date created = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01");
        ObjectId id = ObjectId.get();
        JsObject ragnar = Json.obj(
                $("_id", MongoWrites.objectId(id)),
                $("name", "Ragnard"),
                $("created", MongoWrites.date(created)),
                $("childs", Json.arr($("name", "Bjorn")))
        );
        collection.insertOne(ragnar).one().toCompletableFuture().get();

        JsObject query = Json.obj($("name", MongoWrites.regex("agn")));

        Option<JsValue> searchByDate = collection.find(query)
                .one().toCompletableFuture().get();

        assertThat(searchByDate).isNotEmpty();
        assertThat(searchByDate.get()).isEqualTo(ragnar);

        JsObject query2 = Json.obj($("name", MongoWrites.regex("floki")));

        assertThat(collection.find(query2)
                .one().toCompletableFuture().get()).isEmpty();

    }


    @Test
    public void document_search_by_date() throws ExecutionException, InterruptedException, ParseException {
        MongoCollection<Document> collection = database.getDocumentCollection("doc-vikings");

        Date created = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01");
        ObjectId id = ObjectId.get();
        Document ragnar = new Document().append("_id", id).append("name", "Ragnard").append("created", created);
        Document floki = new Document().append("name", "Floki").append("created", new Date());
        Document rollo = new Document().append("name", "Rollo").append("created", new Date());
        collection.insertMany(Arrays.asList(ragnar, floki, rollo)).one().toCompletableFuture().get();

        Date lowerDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01");
        Date higherDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-02");

        Bson created1 = and(gte("created", lowerDate), lte("created", higherDate));
        List<Document> searchByDate = collection.find(created1)
                .list().toCompletableFuture().get();

        assertThat(searchByDate).isNotEmpty();
        assertThat(searchByDate).hasSize(1);
        assertThat(searchByDate).containsExactly(ragnar);
    }

    @Test
    public void document_write_read__search_long() throws ExecutionException, InterruptedException, ParseException {
        MongoCollection<JsValue> collection = database.getJsonCollection("doc-vikings");

        Date created = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01");
        ObjectId id = ObjectId.get();

        Viking ragnar = new Viking(id.toString(), "Ragnard", List.of(
                new Viking(ObjectId.get().toString(), "Bjorn", List.empty(), 1L)
        ), 2L, created);
        JsValue json = Json.toJson(ragnar, Viking.writes);

        collection.insertOne(json).one().toCompletableFuture().get();

        List<Viking> searchResult = collection.find(Json.obj(
                    $("nbChilds", Json.obj($("$eq", 2L)))
                ))
                .list(reader(Viking.reader)).toCompletableFuture().get();

        assertThat(searchResult).isNotEmpty();
        assertThat(searchResult.head()).isEqualTo(ragnar);

        List<Viking> searchEmptyResult = collection.find(Json.obj(
                $("nbChilds", Json.obj($("$eq", MongoWrites.numberLong(0L))))
        ))
                .list(reader(Viking.reader)).toCompletableFuture().get();

        assertThat(searchEmptyResult).isEmpty();

    }


    @Test
    public void test() throws ExecutionException, InterruptedException, ParseException {

        MongoCollection<JsValue> collection = database.getJsonCollection("collection");

        JsObject ragnar = Json.obj(
                $("name", "Ragnard"),
                $("created", MongoWrites.date(new Date())),
                $("childs", Json.arr($("name", "Bjorn")))
        );

        CompletionStage<Option<Success>> insertStatus = collection.insertOne(ragnar).one();
        insertStatus.toCompletableFuture().get();

        Date created = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-01");
        JsObject floki = Json.obj($("name", "Floki"), $("created", MongoWrites.date(created)));
        JsObject rollo = Json.obj($("name", "Rollo"), $("created", MongoWrites.date(new Date())));
        CompletionStage<Option<Success>> insertManyStatus = collection.insertMany(Arrays.asList(floki, rollo)).one();
        insertManyStatus.toCompletableFuture().get();

        CompletionStage<Option<JsValue>> ragnard = collection.find(Json.obj($("name", MongoWrites.regex("Ragnard")))).one();
        JsValue fromDb = ragnard.toCompletableFuture().get().get();
        assertThat(fromDb.asObject().remove("_id")).isEqualTo(ragnar);

        CompletionStage<List<JsValue>> vikings = collection.find().list();
        List<JsValue> values = vikings.toCompletableFuture().get();

        assertThat(values.map(j -> j.asObject().remove("_id"))).contains(ragnar, floki, rollo);

        CompletionStage<Option<Viking>> mayBeFloki = collection.find(Json.obj($("name", "Floki"))).one(reader(Viking.reader));
        Option<Viking> OptmayBeFloki = mayBeFloki.toCompletableFuture().get();

        assertThat(OptmayBeFloki).isNotEmpty();
        assertThat(OptmayBeFloki.get()).isEqualTo(new Viking(ObjectId.get().toString(), "Floki", List.empty(), 0L, new Date()));

        Date lowerDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-01");
        Date higherDate = new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-02");

        Bson query = gte("created", lowerDate);
        Option<Viking> mayBeFloki2 = collection.find(query)
                .one(reader(Viking.reader)).toCompletableFuture().get();

        assertThat(mayBeFloki2).isNotEmpty();
        assertThat(mayBeFloki2.get()).isEqualTo(new Viking(ObjectId.get().toString(), "Floki", List.empty(), 0L, new Date()));


        Source<Viking, NotUsed> stream = collection.find().stream(reader(Viking.reader));

        ActorMaterializer materializer = ActorMaterializer.create(actorSystem);
        stream.drop(1)
                .runWith(
                        Sink.foreach(viking -> System.out.println(viking)),
                        materializer
                );

    }




    public static class Viking {

        public static Reader<Viking> reader = json ->
            Try.of(() -> JsResult.success(new Viking(
                    json.field("_id").as(MongoReads.strObjectId),
                    json.field("name").asString(),
                    json.field("childs").asOptArray().map(a -> List.ofAll(a).map(Viking::fromJson)).getOrElse(List::empty),
                    json.field("nbChilds").asOpt(MongoReads.numberLong).getOrElse(0L),
                    json.field("created").as(MongoReads.date)
            ))).getOrElseGet(JsResult::error);

        public static Writer<Viking> writes = v -> Json.obj(
                $("_id", MongoWrites.objectId(v.id)),
                $("name", v.name),
                $("nbChilds", v.nbChilds),
                $("childs", Json.array(v.childs.map(c -> Json.toJson(c, Viking.writes)))),
                $("created", Json.toJson(v.created, MongoWrites.date))
        );

        final String id;

        final String name;

        final List<Viking> childs;

        final Long nbChilds;

        final Date created;

        public Viking(String id, String name, List<Viking> childs, Long nbChilds) {
            this.id = id;
            this.name = name;
            this.childs = childs;
            this.nbChilds = nbChilds;
            this.created = new Date();
        }

        public Viking(String id, String name, List<Viking> childs, Long nbChilds, Date created) {
            this.id = id;
            this.name = name;
            this.childs = childs;
            this.nbChilds = nbChilds;
            this.created = created;
        }

        static Viking fromJson(JsValue j) {
            return j.read(Viking.reader).get();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Viking viking = (Viking) o;
            return Objects.equals(name, viking.name) &&
                    Objects.equals(childs, viking.childs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, childs);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("name", name)
                    .append("childs", childs)
                    .toString();
        }
    }

}
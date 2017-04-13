package reactive.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.reactivecouchbase.json.Syntax.$;

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.NotUsed;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import javaslang.collection.List;
import javaslang.control.Try;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;

import akka.actor.ActorSystem;
import javaslang.control.Option;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;

/**
 * Created by adelegue on 12/04/2017.
 */
public class ReactiveMongoClientTest {

    Random random = new Random();

    String databaseName;
    MongoDatabase database;
    MongoClient mongoClient;
    ReactiveMongoClient client;
    ActorSystem actorSystem;

    @Before
    public void setUp() {
        mongoClient = MongoClients.create();
        actorSystem = ActorSystem.create();
        client = ReactiveMongoClient.create(mongoClient, actorSystem);
        databaseName = "test-" + random.nextInt();
        database = client.getDatabase(databaseName);
    }

    @After
    public void cleanUp() throws ExecutionException, InterruptedException {
        database.drop().one().toCompletableFuture().get();
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        MongoCollection collection = database.getCollection("collection");

        JsObject ragnar = Json.obj($("name", "Ragnard"), $("childs", Json.arr($("name", "Bjorn"))));
        CompletionStage<Option<Success>> insertStatus = collection.insertOne(ragnar).one();
        insertStatus.toCompletableFuture().get();
        JsObject floki = Json.obj($("name", "Floki"));
        JsObject rollo = Json.obj($("name", "Rollo"));
        CompletionStage<Option<Success>> insertManyStatus = collection.insertMany(Arrays.asList(floki, rollo)).one();
        insertManyStatus.toCompletableFuture().get();

        CompletionStage<Option<JsValue>> ragnard = collection.find(Json.obj($("name", "Ragnard"))).one();
        JsValue fromDb = ragnard.toCompletableFuture().get().get();
        assertThat(fromDb.asObject().remove("_id")).isEqualTo(ragnar);

        CompletionStage<List<JsValue>> vikings = collection.find().list();
        List<JsValue> values = vikings.toCompletableFuture().get();

        assertThat(values.map(j -> j.asObject().remove("_id"))).contains(ragnar, floki, rollo);

        CompletionStage<Option<Viking>> mayBeFloki = collection.find(Json.obj($("name", "Floki"))).one(Viking.reader);
        Option<Viking> OptmayBeFloki = mayBeFloki.toCompletableFuture().get();

        assertThat(OptmayBeFloki).isNotEmpty();
        assertThat(OptmayBeFloki.get()).isEqualTo(new Viking("Floki", List.empty()));

        Source<Viking, NotUsed> stream = collection.find().stream(Viking.reader);

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
                    json.field("name").asString(),
                    json.field("childs").asOptArray().map(a -> List.ofAll(a).map(Viking::fromJson)).getOrElse(List::empty)
            ))).getOrElseGet(JsResult::error);

        final String name;

        final List<Viking> childs;

        public Viking(String name, List<Viking> childs) {
            this.name = name;
            this.childs = childs;
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
    }

}
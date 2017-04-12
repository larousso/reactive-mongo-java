package reactive.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.reactivecouchbase.json.Syntax.$;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;

import akka.actor.ActorSystem;
import javaslang.control.Option;

/**
 * Created by adelegue on 12/04/2017.
 */
public class ReactiveMongoClientTest {


    @Test
    public void test() throws ExecutionException, InterruptedException {

        MongoClient mongoClient = MongoClients.create();
        ReactiveMongoClient client = ReactiveMongoClient.create(mongoClient, ActorSystem.create());

        MongoDatabase database = client.getDatabase("test");
        MongoCollection collection = database.getCollection("collection");

        JsObject document = Json.obj(
                $("name", "Jean Paul"),
                $("child", $("name", "Jean Phil"))
        );
        Option<Success> name = collection.insertOne(document).one().toCompletableFuture().get();

        JsValue fromDb = collection.find(Json.obj($("name", "Jean Paul"))).one().toCompletableFuture().get().get();
        assertThat(fromDb).isEqualTo(document);
    }


}
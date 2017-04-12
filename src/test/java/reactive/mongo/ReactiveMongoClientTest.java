package reactive.mongo;

import akka.actor.ActorSystem;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;
import javaslang.control.Option;
import org.junit.Test;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import java.util.concurrent.ExecutionException;

import static org.reactivecouchbase.json.Syntax.*;

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

        Option<Success> name = collection.insertOne(Json.obj($("name", "Jean Paul"))).one().toCompletableFuture().get();

        JsValue name1 = collection.find(Json.obj($("name", "Jean Paul"))).one().toCompletableFuture().get().get();

        System.out.println(name1);
    }


}
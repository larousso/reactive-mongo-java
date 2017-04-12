package reactive.mongo;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.Document;
import reactive.mongo.results.SimpleResult;

/**
 * Created by adelegue on 12/04/2017.
 */
public class ReactiveMongoClient {

    final com.mongodb.reactivestreams.client.MongoClient mongoClient;

    private final ActorSystem actorSystem;
    private final Materializer materializer;

    private ReactiveMongoClient(MongoClient mongoClient, ActorSystem actorSystem, Materializer materializer) {
        this.mongoClient = mongoClient;
        this.actorSystem = actorSystem;
        this.materializer = materializer;
    }

    public static ReactiveMongoClient create(com.mongodb.reactivestreams.client.MongoClient mongoClient, ActorSystem actorSystem) {
        return new ReactiveMongoClient(mongoClient, actorSystem, ActorMaterializer.create(actorSystem));
    }

    public MongoDatabase getDatabase(String name) {
        return new MongoDatabase(mongoClient.getDatabase(name), actorSystem, materializer);
    }

    public void close() {
        mongoClient.close();
    }

    public MongoClientSettings getSettings() {
        return mongoClient.getSettings();
    }

    public SimpleResult<String> listDatabaseNames() {
        return new SimpleResult<>(mongoClient.listDatabaseNames(), materializer);
    }

    public SimpleResult<Document> listDatabases() {
        return new SimpleResult<>(mongoClient.listDatabases(), materializer);
    }
}

package reactive.mongo;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import com.mongodb.ConnectionString;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.client.MongoDriverInformation;
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


    /**
     * Creates a new client with the default connection string "mongodb://localhost".
     *
     * @return the client
     */
    public static ReactiveMongoClient create(ActorSystem actorSystem) {
        return create(MongoClients.create(), actorSystem);
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static ReactiveMongoClient create(ActorSystem actorSystem, final MongoClientSettings settings) {
        return create(MongoClients.create(settings), actorSystem);
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the connection
     * @return the client
     */
    public static ReactiveMongoClient create(ActorSystem actorSystem, final String connectionString) {
        return create(MongoClients.create(connectionString), actorSystem);
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the settings
     * @return the client
     */
    public static ReactiveMongoClient create(ActorSystem actorSystem, final ConnectionString connectionString) {
        return create(MongoClients.create(connectionString), actorSystem);
    }


    /**
     * Create a new client with the given connection string.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.3
     */
    public static ReactiveMongoClient create(ActorSystem actorSystem, final ConnectionString connectionString, final MongoDriverInformation mongoDriverInformation) {
        return create(MongoClients.create(connectionString, mongoDriverInformation), actorSystem);
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.3
     */
    public static ReactiveMongoClient create(ActorSystem actorSystem, final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation) {
        return create(MongoClients.create(settings, mongoDriverInformation), actorSystem);
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

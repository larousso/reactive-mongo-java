package reactive.mongo;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivecouchbase.json.JsValue;
import org.reactivestreams.Publisher;
import reactive.mongo.codec.JsValueCodecProvider;
import reactive.mongo.results.SimpleResult;

import java.util.List;

/**
 * Created by adelegue on 12/04/2017.
 */
public class MongoDatabase {

    final com.mongodb.reactivestreams.client.MongoDatabase mongoDatabase;

    final ActorSystem actorSystem;
    private final Materializer materializer;

    public MongoDatabase(com.mongodb.reactivestreams.client.MongoDatabase mongoDatabase, ActorSystem actorSystem, Materializer materializer) {
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(new JsValueCodecProvider()), MongoClients.getDefaultCodecRegistry());
        this.mongoDatabase = mongoDatabase.withCodecRegistry(codecRegistry);
        this.actorSystem = actorSystem;
        this.materializer = materializer;
    }

    public String getName() {
        return mongoDatabase.getName();
    }

    public CodecRegistry getCodecRegistry() {
        return mongoDatabase.getCodecRegistry();
    }

    public ReadPreference getReadPreference() {
        return mongoDatabase.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return mongoDatabase.getWriteConcern();
    }

    public ReadConcern getReadConcern() {
        return mongoDatabase.getReadConcern();
    }

    public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
        return new MongoDatabase(mongoDatabase.withCodecRegistry(codecRegistry), actorSystem, materializer);
    }

    public MongoDatabase withReadPreference(ReadPreference readPreference) {
        return new MongoDatabase(mongoDatabase.withReadPreference(readPreference), actorSystem, materializer);
    }

    public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
        return new MongoDatabase(mongoDatabase.withWriteConcern(writeConcern), actorSystem, materializer);
    }

    public MongoDatabase withReadConcern(ReadConcern readConcern) {
        return new MongoDatabase(mongoDatabase.withReadConcern(readConcern), actorSystem, materializer);
    }

    public MongoCollection<JsValue> getJsonCollection(String collectionName) {
        return new MongoCollection<>(mongoDatabase.getCollection(collectionName, JsValue.class), actorSystem);
    }

    public MongoCollection<Document> getDocumentCollection(String collectionName) {
        return new MongoCollection<>(mongoDatabase.getCollection(collectionName), actorSystem);
    }

    public Publisher<Document> runCommand(Bson command) {
        return mongoDatabase.runCommand(command);
    }

    public Publisher<Document> runCommand(Bson command, ReadPreference readPreference) {
        return mongoDatabase.runCommand(command, readPreference);
    }

    public <TResult> Publisher<TResult> runCommand(Bson command, Class<TResult> clazz) {
        return mongoDatabase.runCommand(command, clazz);
    }

    public <TResult> Publisher<TResult> runCommand(Bson command, ReadPreference readPreference, Class<TResult> clazz) {
        return mongoDatabase.runCommand(command, readPreference, clazz);
    }

    public SimpleResult<Success> drop() {
        return new SimpleResult<>(mongoDatabase.drop(), materializer);
    }

    public SimpleResult<String> listCollectionNames() {
        return new SimpleResult<>(mongoDatabase.listCollectionNames(), materializer);
    }

    public SimpleResult<Document> listCollections() {
        return new SimpleResult<>(mongoDatabase.listCollections(), materializer);
    }

    public SimpleResult<Success> createCollection(String collectionName) {
        return new SimpleResult<>(mongoDatabase.createCollection(collectionName), materializer);
    }

    public SimpleResult<Success> createCollection(String collectionName, CreateCollectionOptions options) {
        return new SimpleResult<>(mongoDatabase.createCollection(collectionName, options), materializer);
    }

    public SimpleResult<Success> createView(String viewName, String viewOn, List<? extends Bson> pipeline) {
        return new SimpleResult<>(mongoDatabase.createView(viewName, viewOn, pipeline), materializer);
    }

    public SimpleResult<Success> createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions) {
        return new SimpleResult<>(mongoDatabase.createView(viewName, viewOn, pipeline, createViewOptions), materializer);
    }
}

package reactive.mongo;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivecouchbase.json.JsValue;
import org.reactivestreams.Publisher;
import reactive.mongo.codec.Conversions;
import reactive.mongo.results.SimpleFindResult;

import java.util.List;

/**
 * Created by 97306p on 12/04/2017.
 */
public class MongoDatabase {

    final com.mongodb.reactivestreams.client.MongoDatabase mongoDatabase;

    final ActorSystem actorSystem;
    private final Conversions conversions;
    private final Materializer materializer;

    public MongoDatabase(com.mongodb.reactivestreams.client.MongoDatabase mongoDatabase, ActorSystem actorSystem, Materializer materializer) {
        this.mongoDatabase = mongoDatabase;
        this.actorSystem = actorSystem;
        this.conversions = new Conversions(new ObjectMapper(), mongoDatabase.getCodecRegistry());
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

    public MongoCollection getCollection(String collectionName) {
        return new MongoCollection(conversions, mongoDatabase.getCollection(collectionName), actorSystem);
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

    public SimpleFindResult<Success> drop() {
        return new SimpleFindResult<>(mongoDatabase.drop(), materializer);
    }

    public SimpleFindResult<String> listCollectionNames() {
        return new SimpleFindResult<>(mongoDatabase.listCollectionNames(), materializer);
    }

    public SimpleFindResult<Document> listCollections() {
        return new SimpleFindResult<>(mongoDatabase.listCollections(), materializer);
    }

    public SimpleFindResult<Success> createCollection(String collectionName) {
        return new SimpleFindResult<>(mongoDatabase.createCollection(collectionName), materializer);
    }

    public SimpleFindResult<Success> createCollection(String collectionName, CreateCollectionOptions options) {
        return new SimpleFindResult<>(mongoDatabase.createCollection(collectionName, options), materializer);
    }

    public SimpleFindResult<Success> createView(String viewName, String viewOn, List<? extends Bson> pipeline) {
        return new SimpleFindResult<>(mongoDatabase.createView(viewName, viewOn, pipeline), materializer);
    }

    public SimpleFindResult<Success> createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions) {
        return new SimpleFindResult<>(mongoDatabase.createView(viewName, viewOn, pipeline, createViewOptions), materializer);
    }
}

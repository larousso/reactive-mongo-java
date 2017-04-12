package reactive.mongo;

import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivecouchbase.json.JsValue;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.*;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import org.reactivestreams.Publisher;
import reactive.mongo.codec.Conversions;
import reactive.mongo.results.FindJsValueResult;
import reactive.mongo.results.SimpleFindResult;
import reactive.mongo.results.SimpleJsValueResult;

/**
 * Created by adelegue on 12/04/2017.
 */
public class MongoCollection {

    private final com.mongodb.reactivestreams.client.MongoCollection<Document> collection;
    private final Materializer materializer;
    private final Conversions conversions;
    private final  ActorSystem system;

    public MongoCollection(Conversions conversions, com.mongodb.reactivestreams.client.MongoCollection<Document> collection, ActorSystem system) {
        this.conversions = conversions;
        this.collection = collection; //.withCodecRegistry(CodecRegistries.fromCodecs(new JsValueCodec(conversions)));
        this.materializer = ActorMaterializer.create(system);
        this.system = system;
    }

    public MongoNamespace getNamespace() {
        return collection.getNamespace();
    }

    public CodecRegistry getCodecRegistry() {
        return collection.getCodecRegistry();
    }

    public ReadPreference getReadPreference() {
        return collection.getReadPreference();
    }

    public WriteConcern getWriteConcern() {
        return collection.getWriteConcern();
    }

    public ReadConcern getReadConcern() {
        return collection.getReadConcern();
    }

    public MongoCollection withCodecRegistry(CodecRegistry codecRegistry) {
        return new MongoCollection(conversions, collection.withCodecRegistry(codecRegistry), system);
    }

    public MongoCollection withReadPreference(ReadPreference readPreference) {
        return new MongoCollection(conversions, collection.withReadPreference(readPreference), system);
    }

    public MongoCollection withWriteConcern(WriteConcern writeConcern) {
        return new MongoCollection(conversions, collection.withWriteConcern(writeConcern), system);
    }

    public MongoCollection withReadConcern(ReadConcern readConcern) {
        return new MongoCollection(conversions, collection.withReadConcern(readConcern), system);
    }

    public SimpleFindResult<Long> count() {
        return new SimpleFindResult<>(collection.count(), materializer);
    }

    public SimpleFindResult<Long> count(JsValue filter) {
        return new SimpleFindResult<>(collection.count(conversions.toBson(filter)), materializer);
    }

    public SimpleFindResult<Long> count(JsValue filter, CountOptions options) {
        return new SimpleFindResult<>(collection.count(conversions.toBson(filter), options), materializer);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, tResultClass);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, JsValue filter, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, conversions.toBson(filter), tResultClass);
    }

    public FindJsValueResult find() {
        return new FindJsValueResult(collection.find(), conversions, materializer);
    }

    public FindJsValueResult find(JsValue filter) {
        return new FindJsValueResult(collection.find(conversions.toBson(filter)), conversions, materializer);
    }

    public AggregatePublisher<Document> aggregate(List<? extends Bson> pipeline) {
        return collection.aggregate(pipeline);
    }

    public <TResult> AggregatePublisher<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> clazz) {
        return collection.aggregate(pipeline, clazz);
    }

    public MapReducePublisher<Document> mapReduce(String mapFunction, String reduceFunction) {
        return collection.mapReduce(mapFunction, reduceFunction);
    }

    public <TResult> MapReducePublisher<TResult> mapReduce(String mapFunction, String reduceFunction, Class<TResult> clazz) {
        return collection.mapReduce(mapFunction, reduceFunction, clazz);
    }

    public SimpleFindResult<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends Document>> requests) {
        return new SimpleFindResult<>(collection.bulkWrite(requests), materializer);
    }

    public SimpleFindResult<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends Document>> requests, BulkWriteOptions options) {
        return new SimpleFindResult<>(collection.bulkWrite(requests, options), materializer);
    }

    public SimpleFindResult<Success> insertOne(JsValue jsValue) {
        return new SimpleFindResult<>(collection.insertOne(conversions.toDocument(jsValue)), materializer);
    }

    public SimpleFindResult<Success> insertOne(JsValue jsValue, InsertOneOptions options) {
        return new SimpleFindResult<>(collection.insertOne(conversions.toDocument(jsValue), options), materializer);
    }

    public SimpleFindResult<Success> insertMany(List<? extends JsValue> jsValues) {
        List<Document> documents = javaslang.collection.List.ofAll(jsValues).map(conversions::toDocument).toJavaList();
        return new SimpleFindResult<>(collection.insertMany(documents), materializer);
    }

    public SimpleFindResult<Success> insertMany(List<? extends JsValue> jsValues, InsertManyOptions options) {
        List<Document> documents = javaslang.collection.List.ofAll(jsValues).map(conversions::toDocument).toJavaList();
        return new SimpleFindResult<>(collection.insertMany(documents, options), materializer);
    }

    public SimpleFindResult<DeleteResult> deleteOne(JsValue filter) {
        return new SimpleFindResult<>(collection.deleteOne(conversions.toBson(filter)), materializer);
    }

    public SimpleFindResult<DeleteResult> deleteMany(JsValue filter) {
        return new SimpleFindResult<>(collection.deleteMany(conversions.toBson(filter)), materializer);
    }

    public SimpleFindResult<UpdateResult> replaceOne(JsValue filter, JsValue replacement) {
        return new SimpleFindResult<>(collection.replaceOne(conversions.toBson(filter), conversions.toDocument(replacement)), materializer);
    }

    public SimpleFindResult<UpdateResult> replaceOne(JsValue filter, JsValue replacement, UpdateOptions options) {
        return new SimpleFindResult<>(collection.replaceOne(conversions.toBson(filter), conversions.toDocument(replacement), options), materializer);
    }

    public SimpleFindResult<UpdateResult> updateOne(JsValue filter, JsValue update) {
        return new SimpleFindResult<>(collection.updateOne(conversions.toBson(filter), conversions.toBson(update)), materializer);
    }

    public SimpleFindResult<UpdateResult> updateOne(JsValue filter, JsValue update, UpdateOptions options) {
        return new SimpleFindResult<>(collection.updateOne(conversions.toBson(filter), conversions.toBson(update), options), materializer);
    }

    public SimpleFindResult<UpdateResult> updateMany(JsValue filter, JsValue update) {
        return new SimpleFindResult<>(collection.updateMany(conversions.toBson(filter), conversions.toBson(update)), materializer);
    }

    public SimpleFindResult<UpdateResult> updateMany(JsValue filter, JsValue update, UpdateOptions options) {
        return new SimpleFindResult<>(collection.updateMany(conversions.toBson(filter), conversions.toBson(update), options), materializer);
    }

    public SimpleJsValueResult findOneAndDelete(JsValue filter) {
        Publisher<Document> oneAndDelete = collection.findOneAndDelete(conversions.toBson(filter));
        return new SimpleJsValueResult(oneAndDelete, conversions, materializer);
    }

    public SimpleJsValueResult findOneAndDelete(JsValue filter, FindOneAndDeleteOptions options) {
        return new SimpleJsValueResult(collection.findOneAndDelete(conversions.toBson(filter), options), conversions, materializer);
    }

    public SimpleJsValueResult findOneAndReplace(JsValue filter, JsValue replacement) {
        return new SimpleJsValueResult(collection.findOneAndReplace(conversions.toBson(filter), conversions.toDocument(replacement)), conversions, materializer);
    }

    public SimpleJsValueResult findOneAndReplace(JsValue filter, JsValue replacement, FindOneAndReplaceOptions options) {
        return new SimpleJsValueResult(collection.findOneAndReplace(conversions.toBson(filter), conversions.toDocument(replacement), options), conversions, materializer);
    }

    public SimpleJsValueResult findOneAndUpdate(JsValue filter, JsValue update) {
        return new SimpleJsValueResult(collection.findOneAndUpdate(conversions.toBson(filter), conversions.toBson(update)), conversions, materializer);
    }

    public SimpleJsValueResult findOneAndUpdate(JsValue filter, JsValue update, FindOneAndUpdateOptions options) {
        return new SimpleJsValueResult(collection.findOneAndUpdate(conversions.toBson(filter), conversions.toBson(update), options), conversions, materializer);
    }

    public SimpleFindResult<Success> drop() {
        return new SimpleFindResult<>(collection.drop(), materializer);
    }

    public SimpleFindResult<String> createIndex(JsValue key) {
        return new SimpleFindResult<>(collection.createIndex(conversions.toBson(key)), materializer);
    }

    public SimpleFindResult<String> createIndex(JsValue key, IndexOptions options) {
        return new SimpleFindResult<>(collection.createIndex(conversions.toBson(key), options), materializer);
    }

    public SimpleFindResult<String> createIndexes(List<IndexModel> indexes) {
        return new SimpleFindResult<>(collection.createIndexes(indexes), materializer);
    }

    public ListIndexesPublisher<Document> listIndexes() {
        return collection.listIndexes();
    }

    public SimpleFindResult<Success> dropIndex(String indexName) {
        return new SimpleFindResult<>(collection.dropIndex(indexName), materializer);
    }

    public SimpleFindResult<Success> dropIndex(JsValue keys) {
        return new SimpleFindResult<>(collection.dropIndex(conversions.toBson(keys)), materializer);
    }

    public SimpleFindResult<Success> dropIndexes() {
        return new SimpleFindResult<>(collection.dropIndexes(), materializer);
    }

    public SimpleFindResult<Success> renameCollection(MongoNamespace newCollectionNamespace) {
        return new SimpleFindResult<>(collection.renameCollection(newCollectionNamespace), materializer);
    }

    public SimpleFindResult<Success> renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions options) {
        return new SimpleFindResult<>(collection.renameCollection(newCollectionNamespace, options), materializer);
    }

}

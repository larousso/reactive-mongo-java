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
import reactive.mongo.results.FindJsonResult;
import reactive.mongo.results.SimpleResult;
import reactive.mongo.results.JsonResult;

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
        this.collection = collection;
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

    public SimpleResult<Long> count() {
        return new SimpleResult<>(collection.count(), materializer);
    }

    public SimpleResult<Long> count(JsValue filter) {
        return new SimpleResult<>(collection.count(conversions.toBson(filter)), materializer);
    }

    public SimpleResult<Long> count(JsValue filter, CountOptions options) {
        return new SimpleResult<>(collection.count(conversions.toBson(filter), options), materializer);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, tResultClass);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, JsValue filter, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, conversions.toBson(filter), tResultClass);
    }

    public FindJsonResult find() {
        return new FindJsonResult(collection.find(), conversions, materializer);
    }

    public FindJsonResult find(JsValue filter) {
        return new FindJsonResult(collection.find(conversions.toBson(filter)), conversions, materializer);
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

    public SimpleResult<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends Document>> requests) {
        return new SimpleResult<>(collection.bulkWrite(requests), materializer);
    }

    public SimpleResult<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends Document>> requests, BulkWriteOptions options) {
        return new SimpleResult<>(collection.bulkWrite(requests, options), materializer);
    }

    public SimpleResult<Success> insertOne(JsValue jsValue) {
        return new SimpleResult<>(collection.insertOne(conversions.toDocument(jsValue)), materializer);
    }

    public SimpleResult<Success> insertOne(JsValue jsValue, InsertOneOptions options) {
        return new SimpleResult<>(collection.insertOne(conversions.toDocument(jsValue), options), materializer);
    }

    public SimpleResult<Success> insertMany(List<? extends JsValue> jsValues) {
        List<Document> documents = javaslang.collection.List.ofAll(jsValues).map(conversions::toDocument).toJavaList();
        return new SimpleResult<>(collection.insertMany(documents), materializer);
    }

    public SimpleResult<Success> insertMany(List<? extends JsValue> jsValues, InsertManyOptions options) {
        List<Document> documents = javaslang.collection.List.ofAll(jsValues).map(conversions::toDocument).toJavaList();
        return new SimpleResult<>(collection.insertMany(documents, options), materializer);
    }

    public SimpleResult<DeleteResult> deleteOne(JsValue filter) {
        return new SimpleResult<>(collection.deleteOne(conversions.toBson(filter)), materializer);
    }

    public SimpleResult<DeleteResult> deleteMany(JsValue filter) {
        return new SimpleResult<>(collection.deleteMany(conversions.toBson(filter)), materializer);
    }

    public SimpleResult<UpdateResult> replaceOne(JsValue filter, JsValue replacement) {
        return new SimpleResult<>(collection.replaceOne(conversions.toBson(filter), conversions.toDocument(replacement)), materializer);
    }

    public SimpleResult<UpdateResult> replaceOne(JsValue filter, JsValue replacement, UpdateOptions options) {
        return new SimpleResult<>(collection.replaceOne(conversions.toBson(filter), conversions.toDocument(replacement), options), materializer);
    }

    public SimpleResult<UpdateResult> updateOne(JsValue filter, JsValue update) {
        return new SimpleResult<>(collection.updateOne(conversions.toBson(filter), conversions.toBson(update)), materializer);
    }

    public SimpleResult<UpdateResult> updateOne(JsValue filter, JsValue update, UpdateOptions options) {
        return new SimpleResult<>(collection.updateOne(conversions.toBson(filter), conversions.toBson(update), options), materializer);
    }

    public SimpleResult<UpdateResult> updateMany(JsValue filter, JsValue update) {
        return new SimpleResult<>(collection.updateMany(conversions.toBson(filter), conversions.toBson(update)), materializer);
    }

    public SimpleResult<UpdateResult> updateMany(JsValue filter, JsValue update, UpdateOptions options) {
        return new SimpleResult<>(collection.updateMany(conversions.toBson(filter), conversions.toBson(update), options), materializer);
    }

    public JsonResult findOneAndDelete(JsValue filter) {
        Publisher<Document> oneAndDelete = collection.findOneAndDelete(conversions.toBson(filter));
        return new JsonResult(oneAndDelete, conversions, materializer);
    }

    public JsonResult findOneAndDelete(JsValue filter, FindOneAndDeleteOptions options) {
        return new JsonResult(collection.findOneAndDelete(conversions.toBson(filter), options), conversions, materializer);
    }

    public JsonResult findOneAndReplace(JsValue filter, JsValue replacement) {
        return new JsonResult(collection.findOneAndReplace(conversions.toBson(filter), conversions.toDocument(replacement)), conversions, materializer);
    }

    public JsonResult findOneAndReplace(JsValue filter, JsValue replacement, FindOneAndReplaceOptions options) {
        return new JsonResult(collection.findOneAndReplace(conversions.toBson(filter), conversions.toDocument(replacement), options), conversions, materializer);
    }

    public JsonResult findOneAndUpdate(JsValue filter, JsValue update) {
        return new JsonResult(collection.findOneAndUpdate(conversions.toBson(filter), conversions.toBson(update)), conversions, materializer);
    }

    public JsonResult findOneAndUpdate(JsValue filter, JsValue update, FindOneAndUpdateOptions options) {
        return new JsonResult(collection.findOneAndUpdate(conversions.toBson(filter), conversions.toBson(update), options), conversions, materializer);
    }

    public SimpleResult<Success> drop() {
        return new SimpleResult<>(collection.drop(), materializer);
    }

    public SimpleResult<String> createIndex(JsValue key) {
        return new SimpleResult<>(collection.createIndex(conversions.toBson(key)), materializer);
    }

    public SimpleResult<String> createIndex(JsValue key, IndexOptions options) {
        return new SimpleResult<>(collection.createIndex(conversions.toBson(key), options), materializer);
    }

    public SimpleResult<String> createIndexes(List<IndexModel> indexes) {
        return new SimpleResult<>(collection.createIndexes(indexes), materializer);
    }

    public ListIndexesPublisher<Document> listIndexes() {
        return collection.listIndexes();
    }

    public SimpleResult<Success> dropIndex(String indexName) {
        return new SimpleResult<>(collection.dropIndex(indexName), materializer);
    }

    public SimpleResult<Success> dropIndex(JsValue keys) {
        return new SimpleResult<>(collection.dropIndex(conversions.toBson(keys)), materializer);
    }

    public SimpleResult<Success> dropIndexes() {
        return new SimpleResult<>(collection.dropIndexes(), materializer);
    }

    public SimpleResult<Success> renameCollection(MongoNamespace newCollectionNamespace) {
        return new SimpleResult<>(collection.renameCollection(newCollectionNamespace), materializer);
    }

    public SimpleResult<Success> renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions options) {
        return new SimpleResult<>(collection.renameCollection(newCollectionNamespace, options), materializer);
    }

}

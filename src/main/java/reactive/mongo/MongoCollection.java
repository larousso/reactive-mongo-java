package reactive.mongo;

import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivecouchbase.json.JsObject;
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

import javax.print.Doc;

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

    public SimpleResult<Long> count(JsObject filter) {
        return new SimpleResult<>(collection.count(conversions.toBson(filter)), materializer);
    }

    public SimpleResult<Long> count(JsObject filter, CountOptions options) {
        return new SimpleResult<>(collection.count(conversions.toBson(filter), options), materializer);
    }
    public SimpleResult<Long> count(Bson filter) {
        return new SimpleResult<>(collection.count(filter), materializer);
    }

    public SimpleResult<Long> count(Bson filter, CountOptions options) {
        return new SimpleResult<>(collection.count(filter, options), materializer);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, tResultClass);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, JsObject filter, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, conversions.toBson(filter), tResultClass);
    }

    public <TResult> DistinctPublisher<TResult> distinct(String fieldName, Bson filter, Class<TResult> tResultClass) {
        return collection.distinct(fieldName, filter, tResultClass);
    }

    public FindJsonResult find() {
        return new FindJsonResult(collection.find(), conversions, materializer);
    }

    public FindJsonResult find(JsObject filter) {
        return new FindJsonResult(collection.find(conversions.toBson(filter)), conversions, materializer);
    }

    public FindJsonResult find(Bson filter) {
        return new FindJsonResult(collection.find(filter), conversions, materializer);
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

    public SimpleResult<Success> insertOne(Document jsValue) {
        return new SimpleResult<>(collection.insertOne(jsValue), materializer);
    }

    public SimpleResult<Success> insertOne(Document jsValue, InsertOneOptions options) {
        return new SimpleResult<>(collection.insertOne(jsValue, options), materializer);
    }

    public SimpleResult<Success> insertMany(List<? extends JsValue> jsValues) {
        List<Document> documents = javaslang.collection.List.ofAll(jsValues).map(conversions::toDocument).toJavaList();
        return new SimpleResult<>(collection.insertMany(documents), materializer);
    }

    public SimpleResult<Success> insertMany(List<? extends JsValue> jsValues, InsertManyOptions options) {
        List<Document> documents = javaslang.collection.List.ofAll(jsValues).map(conversions::toDocument).toJavaList();
        return new SimpleResult<>(collection.insertMany(documents, options), materializer);
    }

    public SimpleResult<Success> insertManyRaw(List<Document> documents) {
        return new SimpleResult<>(collection.insertMany(documents), materializer);
    }

    public SimpleResult<Success> insertManyRaw(List<Document> documents, InsertManyOptions options) {
        return new SimpleResult<>(collection.insertMany(documents, options), materializer);
    }

    public SimpleResult<DeleteResult> deleteOne(JsObject filter) {
        return new SimpleResult<>(collection.deleteOne(conversions.toBson(filter)), materializer);
    }

    public SimpleResult<DeleteResult> deleteMany(JsObject filter) {
        return new SimpleResult<>(collection.deleteMany(conversions.toBson(filter)), materializer);
    }

    public SimpleResult<DeleteResult> deleteOne(Bson filter) {
        return new SimpleResult<>(collection.deleteOne(filter), materializer);
    }

    public SimpleResult<DeleteResult> deleteMany(Bson filter) {
        return new SimpleResult<>(collection.deleteMany(filter), materializer);
    }

    public SimpleResult<UpdateResult> replaceOne(JsObject filter, JsValue replacement) {
        return new SimpleResult<>(collection.replaceOne(conversions.toBson(filter), conversions.toDocument(replacement)), materializer);
    }

    public SimpleResult<UpdateResult> replaceOne(JsObject filter, JsValue replacement, UpdateOptions options) {
        return new SimpleResult<>(collection.replaceOne(conversions.toBson(filter), conversions.toDocument(replacement), options), materializer);
    }
    public SimpleResult<UpdateResult> replaceOne(Bson filter, Document replacement) {
        return new SimpleResult<>(collection.replaceOne(filter, replacement), materializer);
    }

    public SimpleResult<UpdateResult> replaceOne(Bson filter, Document replacement, UpdateOptions options) {
        return new SimpleResult<>(collection.replaceOne(filter, replacement, options), materializer);
    }

    public SimpleResult<UpdateResult> updateOne(JsObject filter, JsObject update) {
        return new SimpleResult<>(collection.updateOne(conversions.toBson(filter), conversions.toBson(update)), materializer);
    }

    public SimpleResult<UpdateResult> updateOne(JsObject filter, JsObject update, UpdateOptions options) {
        return new SimpleResult<>(collection.updateOne(conversions.toBson(filter), conversions.toBson(update), options), materializer);
    }

    public SimpleResult<UpdateResult> updateOne(Bson filter, Document update) {
        return new SimpleResult<>(collection.updateOne(filter, update), materializer);
    }

    public SimpleResult<UpdateResult> updateOne(Bson filter, Document update, UpdateOptions options) {
        return new SimpleResult<>(collection.updateOne(filter, update, options), materializer);
    }

    public SimpleResult<UpdateResult> updateMany(JsObject filter, JsObject update) {
        return new SimpleResult<>(collection.updateMany(conversions.toBson(filter), conversions.toBson(update)), materializer);
    }

    public SimpleResult<UpdateResult> updateMany(JsObject filter, JsObject update, UpdateOptions options) {
        return new SimpleResult<>(collection.updateMany(conversions.toBson(filter), conversions.toBson(update), options), materializer);
    }

    public SimpleResult<UpdateResult> updateMany(Bson filter, Document update) {
        return new SimpleResult<>(collection.updateMany(filter, update), materializer);
    }

    public SimpleResult<UpdateResult> updateMany(Bson filter, Document update, UpdateOptions options) {
        return new SimpleResult<>(collection.updateMany(filter, update, options), materializer);
    }

    public JsonResult findOneAndDelete(JsObject filter) {
        Publisher<Document> oneAndDelete = collection.findOneAndDelete(conversions.toBson(filter));
        return new JsonResult(oneAndDelete, conversions, materializer);
    }

    public JsonResult findOneAndDelete(JsObject filter, FindOneAndDeleteOptions options) {
        return new JsonResult(collection.findOneAndDelete(conversions.toBson(filter), options), conversions, materializer);
    }

    public JsonResult findOneAndDelete(Bson filter) {
        Publisher<Document> oneAndDelete = collection.findOneAndDelete(filter);
        return new JsonResult(oneAndDelete, conversions, materializer);
    }

    public JsonResult findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
        return new JsonResult(collection.findOneAndDelete(filter, options), conversions, materializer);
    }

    public JsonResult findOneAndReplace(JsObject filter, JsValue replacement) {
        return new JsonResult(collection.findOneAndReplace(conversions.toBson(filter), conversions.toDocument(replacement)), conversions, materializer);
    }

    public JsonResult findOneAndReplace(JsObject filter, JsValue replacement, FindOneAndReplaceOptions options) {
        return new JsonResult(collection.findOneAndReplace(conversions.toBson(filter), conversions.toDocument(replacement), options), conversions, materializer);
    }

    public JsonResult findOneAndReplace(Bson filter, Document replacement) {
        return new JsonResult(collection.findOneAndReplace(filter, replacement), conversions, materializer);
    }

    public JsonResult findOneAndReplace(Bson filter, Document replacement, FindOneAndReplaceOptions options) {
        return new JsonResult(collection.findOneAndReplace(filter, replacement, options), conversions, materializer);
    }

    public JsonResult findOneAndUpdate(JsObject filter, JsObject update) {
        return new JsonResult(collection.findOneAndUpdate(conversions.toBson(filter), conversions.toBson(update)), conversions, materializer);
    }

    public JsonResult findOneAndUpdate(JsObject filter, JsObject update, FindOneAndUpdateOptions options) {
        return new JsonResult(collection.findOneAndUpdate(conversions.toBson(filter), conversions.toBson(update), options), conversions, materializer);
    }

    public JsonResult findOneAndUpdate(Bson filter, JsObject update) {
        return new JsonResult(collection.findOneAndUpdate(filter, conversions.toBson(update)), conversions, materializer);
    }

    public JsonResult findOneAndUpdate(Bson filter, JsObject update, FindOneAndUpdateOptions options) {
        return new JsonResult(collection.findOneAndUpdate(filter, conversions.toBson(update), options), conversions, materializer);
    }

    public SimpleResult<Success> drop() {
        return new SimpleResult<>(collection.drop(), materializer);
    }

    public SimpleResult<String> createIndex(JsObject key) {
        return new SimpleResult<>(collection.createIndex(conversions.toBson(key)), materializer);
    }

    public SimpleResult<String> createIndex(JsObject key, IndexOptions options) {
        return new SimpleResult<>(collection.createIndex(conversions.toBson(key), options), materializer);
    }

    public SimpleResult<String> createIndexes(List<IndexModel> indexes) {
        return new SimpleResult<>(collection.createIndexes(indexes), materializer);
    }

    public SimpleResult<String> createIndex(Document key) {
        return new SimpleResult<>(collection.createIndex(key), materializer);
    }

    public SimpleResult<String> createIndex(Document key, IndexOptions options) {
        return new SimpleResult<>(collection.createIndex(key, options), materializer);
    }

    public ListIndexesPublisher<Document> listIndexes() {
        return collection.listIndexes();
    }

    public SimpleResult<Success> dropIndex(String indexName) {
        return new SimpleResult<>(collection.dropIndex(indexName), materializer);
    }

    public SimpleResult<Success> dropIndex(JsObject keys) {
        return new SimpleResult<>(collection.dropIndex(conversions.toBson(keys)), materializer);
    }

    public SimpleResult<Success> dropIndex(Document keys) {
        return new SimpleResult<>(collection.dropIndex(keys), materializer);
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

    public com.mongodb.reactivestreams.client.MongoCollection<Document> rawCollection() {
        return collection;
    }
}

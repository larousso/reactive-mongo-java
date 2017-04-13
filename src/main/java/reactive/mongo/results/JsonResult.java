package reactive.mongo.results;

import java.util.concurrent.CompletionStage;

import akka.stream.javadsl.Flow;
import javaslang.collection.List;
import org.bson.Document;
import org.reactivecouchbase.json.JsValue;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import javaslang.control.Option;
import org.reactivecouchbase.json.mapping.Reader;
import org.reactivestreams.Publisher;
import reactive.mongo.codec.Conversions;

/**
 * Created by adelegue on 12/04/2017.
 */
public class JsonResult {

    final Publisher<Document> result;
    final Materializer materializer;
    final Conversions conversions;

    public JsonResult(Publisher<Document> result, Conversions conversions, Materializer materializer) {
        this.result = result;
        this.conversions = conversions;
        this.materializer = materializer;
    }

    public CompletionStage<Option<JsValue>> one() {
        return stream()
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }

    public <T> CompletionStage<Option<T>> one(Reader<T> reader) {
        return stream(reader)
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }


    public CompletionStage<List<JsValue>> list() {
        return stream()
                .runWith(Sink.seq(), materializer)
                .thenApply(List::ofAll);
    }


    public <T> CompletionStage<List<T>> list(Reader<T> reader) {
        return stream(reader)
                .runWith(Sink.seq(), materializer)
                .thenApply(List::ofAll);
    }

    public Source<JsValue, NotUsed> stream() {
        return Source
                .fromPublisher(this.result)
                .via(toJson());
    }


    public <T> Source<T, NotUsed> stream(Reader<T> reader) {
        return stream()
                .via(toObj(reader));
    }

    protected Flow<Document, JsValue, NotUsed> toJson(){
        return Flow.<Document>create().map(conversions::fromDocument);
    }

    protected <T> Flow<JsValue, T, NotUsed> toObj(Reader<T> reader){
        return Flow.<JsValue>create().map(json -> json.read(reader).get());
    }

}

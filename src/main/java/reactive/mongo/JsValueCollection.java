package reactive.mongo;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import javaslang.control.Option;
import org.bson.Document;
import org.reactivecouchbase.json.JsValue;
import reactive.mongo.conversion.Conversions;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Created by adelegue on 12/04/2017.
 */
public class JsValueCollection {

    MongoCollection<Document> collection;


    public JsValueCollection(MongoCollection<Document> collection) {
        this.collection = collection;
    }


    public static class FindResult {

        FindPublisher<Document> result;

        Materializer materializer;

        public FindResult(FindPublisher<Document> result, Materializer materializer) {
            this.result = result;
            this.materializer = materializer;
        }

        public CompletionStage<Option<JsValue>> one() {
            return Source
                    .fromPublisher(this.result.first())
                    .map(Conversions::fromDocument)
                    .runWith(Sink.headOption(), materializer)
                    .thenApply(Option::ofOptional);
        }

        public CompletionStage<List<JsValue>> list() {
            return Source
                    .fromPublisher(this.result)
                    .map(Conversions::fromDocument)
                    .runWith(Sink.seq(), materializer);
        }

        public Source<JsValue, NotUsed> source() {
            return Source
                    .fromPublisher(this.result)
                    .map(Conversions::fromDocument);
        }

    }

}

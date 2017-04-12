package reactive.mongo.results;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.bson.Document;
import org.reactivecouchbase.json.JsValue;

import com.mongodb.reactivestreams.client.FindPublisher;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import javaslang.control.Option;
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
        return Source
                .fromPublisher(this.result)
                .map(conversions::fromDocument)
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }

    public CompletionStage<List<JsValue>> list() {
        return Source
                .fromPublisher(this.result)
                .map(conversions::fromDocument)
                .runWith(Sink.seq(), materializer);
    }

    public Source<JsValue, NotUsed> source() {
        return Source
                .fromPublisher(this.result)
                .map(conversions::fromDocument);
    }

}

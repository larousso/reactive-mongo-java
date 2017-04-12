package reactive.mongo.results;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.mongodb.reactivestreams.client.FindPublisher;
import javaslang.control.Option;
import org.bson.Document;
import org.reactivecouchbase.json.JsValue;
import reactive.mongo.codec.Conversions;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Created by 97306p on 12/04/2017.
 */
public class FindJsValueResult extends SimpleJsValueResult {

    private final FindPublisher<Document> result;
    private final Materializer materializer;
    private final Conversions conversions;

    public FindJsValueResult(FindPublisher<Document> result, Conversions conversions, Materializer materializer) {
        super(result, conversions, materializer);
        this.result = result;
        this.conversions = conversions;
        this.materializer = materializer;
    }

    public CompletionStage<Option<JsValue>> one() {
        return Source
                .fromPublisher(this.result.first())
                .map(conversions::fromDocument)
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }

}

package reactive.mongo.results;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Created by adelegue on 12/04/2017.
 */
public class SimpleResult<T> {
    private final Publisher<T> result;
    private final Materializer materializer;

    public SimpleResult(Publisher<T> result, Materializer materializer) {
        this.result = result;
        this.materializer = materializer;
    }

    public CompletionStage<Option<T>> one() {
        return stream()
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }

    public CompletionStage<List<T>> list() {
        return stream()
                .runWith(Sink.seq(), materializer);
    }

    public Source<T, NotUsed> stream() {
        return Source.fromPublisher(this.result);
    }

}

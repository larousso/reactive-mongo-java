package reactive.mongo.results;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

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

    public Future<Option<T>> one() {
        return Future.fromCompletableFuture(stream().runWith(Sink.headOption(), materializer).toCompletableFuture())
                .map(Option::ofOptional);
    }

    public Future<List<T>> list() {
        return Future.fromCompletableFuture(stream().runWith(Sink.seq(), materializer).toCompletableFuture())
                .map(List::ofAll);
    }

    public Source<T, NotUsed> stream() {
        return Source.fromPublisher(this.result);
    }

}

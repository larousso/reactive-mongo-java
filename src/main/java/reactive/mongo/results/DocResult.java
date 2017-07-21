package reactive.mongo.results;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;
import reactive.mongo.DocReader;


/**
 * Created by adelegue on 12/04/2017.
 */
public class DocResult<DOC> {

    final Publisher<DOC> result;
    final Materializer materializer;

    public DocResult(Publisher<DOC> result, Materializer materializer) {
        this.result = result;
        this.materializer = materializer;
    }

    public Future<Option<DOC>> one() {
        return Future.fromCompletableFuture(stream().runWith(Sink.headOption(), materializer).toCompletableFuture())
                .map(Option::ofOptional);
    }

    public <T> Future<Option<T>> one(DocReader<DOC, T> reader) {
        return Future.fromCompletableFuture(stream(reader).runWith(Sink.headOption(), materializer).toCompletableFuture())
                .map(Option::ofOptional);
    }


    public Future<List<DOC>> list() {
        return Future.fromCompletableFuture(stream().runWith(Sink.seq(), materializer).toCompletableFuture())
                .map(List::ofAll);
    }


    public <T> Future<List<T>> list(DocReader<DOC, T> reader) {
        return Future.fromCompletableFuture(stream(reader).runWith(Sink.seq(), materializer).toCompletableFuture())
                .map(List::ofAll);
    }

    public Source<DOC, NotUsed> stream() {
        return Source
                .fromPublisher(this.result);
    }


    public <T> Source<T, NotUsed> stream(DocReader<DOC, T> reader) {
        return stream().via(toObj(reader));
    }

    protected <T> Flow<DOC, T, NotUsed> toObj(DocReader<DOC, T> reader){
        return Flow.<DOC>create().map(reader::read).mapConcat(e -> e);
    }

}

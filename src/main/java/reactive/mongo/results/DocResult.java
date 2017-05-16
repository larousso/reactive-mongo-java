package reactive.mongo.results;

import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import javaslang.collection.List;
import javaslang.control.Option;
import org.reactivestreams.Publisher;
import reactive.mongo.DocReader;

import java.util.concurrent.CompletionStage;

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

    public CompletionStage<Option<DOC>> one() {
        return stream()
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }

    public <T> CompletionStage<Option<T>> one(DocReader<DOC, T> reader) {
        return stream(reader)
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }


    public CompletionStage<List<DOC>> list() {
        return stream()
                .runWith(Sink.seq(), materializer)
                .thenApply(List::ofAll);
    }


    public <T> CompletionStage<List<T>> list(DocReader<DOC, T> reader) {
        return stream(reader)
                .runWith(Sink.seq(), materializer)
                .thenApply(List::ofAll);
    }

    public Source<DOC, NotUsed> stream() {
        return Source
                .fromPublisher(this.result);
    }


    public <T> Source<T, NotUsed> stream(DocReader<DOC, T> reader) {
        return stream()
                .via(toObj(reader));
    }

    protected <T> Flow<DOC, T, NotUsed> toObj(DocReader<DOC, T> reader){
        return Flow.<DOC>create().map(reader::read).mapConcat(e -> e);
    }

}

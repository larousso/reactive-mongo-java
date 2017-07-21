package reactive.mongo.results;

import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import com.mongodb.CursorType;
import com.mongodb.client.model.Collation;
import com.mongodb.reactivestreams.client.FindPublisher;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.bson.conversions.Bson;
import reactive.mongo.DocReader;

import java.util.concurrent.TimeUnit;

/**
 * Created by adelegue on 12/04/2017.
 */
public class FindJsonResult<DOC> extends DocResult<DOC> {

    private final FindPublisher<DOC> result;

    public FindJsonResult(FindPublisher<DOC> result, Materializer materializer) {
        super(result, materializer);
        this.result = result;
    }

    @Override
    public Future<Option<DOC>> one() {
        return Future.fromCompletableFuture(stream().runWith(Sink.headOption(), materializer).toCompletableFuture())
                .map(Option::ofOptional);
    }


    @Override
    public <T> Future<Option<T>> one(DocReader<DOC, T> reader) {
        return Future.fromCompletableFuture(stream()
                .via(toObj(reader))
                .runWith(Sink.headOption(), materializer).toCompletableFuture())
                .map(Option::ofOptional);
    }


    public FindJsonResult filter(Bson filter) {
        return new FindJsonResult(result.filter(filter), materializer);
    }

    public FindJsonResult limit(int limit) {
        return new FindJsonResult(result.limit(limit), materializer);
    }

    public FindJsonResult skip(int skip) {
        return new FindJsonResult(result.skip(skip), materializer);
    }

    public FindJsonResult maxTime(long maxTime, TimeUnit timeUnit) {
        return new FindJsonResult(result.maxTime(maxTime, timeUnit), materializer);
    }

    public FindJsonResult maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        return new FindJsonResult(result.maxAwaitTime(maxAwaitTime, timeUnit), materializer);
    }

    public FindJsonResult modifiers(Bson modifiers) {
        return new FindJsonResult(result.modifiers(modifiers), materializer);
    }

    public FindJsonResult projection(Bson projection) {
        return new FindJsonResult(result.projection(projection), materializer);
    }

    public FindJsonResult sort(Bson sort) {
        return new FindJsonResult(result.sort(sort), materializer);
    }

    public FindJsonResult noCursorTimeout(boolean noCursorTimeout) {
        return new FindJsonResult(result.noCursorTimeout(noCursorTimeout), materializer);
    }

    public FindJsonResult oplogReplay(boolean oplogReplay) {
        return new FindJsonResult(result.oplogReplay(oplogReplay), materializer);
    }

    public FindJsonResult partial(boolean partial) {
        return new FindJsonResult(result.partial(partial), materializer);
    }

    public FindJsonResult cursorType(CursorType cursorType) {
        return new FindJsonResult(result.cursorType(cursorType), materializer);
    }

    public FindJsonResult collation(Collation collation) {
        return new FindJsonResult(result.collation(collation), materializer);
    }
}

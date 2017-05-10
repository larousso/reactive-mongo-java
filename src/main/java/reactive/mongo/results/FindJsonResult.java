package reactive.mongo.results;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.bson.conversions.Bson;

import com.mongodb.CursorType;
import com.mongodb.client.model.Collation;
import com.mongodb.reactivestreams.client.FindPublisher;

import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import javaslang.control.Option;
import reactive.mongo.DocReader;
import reactive.mongo.codec.tmp.Conversions;

/**
 * Created by adelegue on 12/04/2017.
 */
public class FindJsonResult<DOC> extends DocResult<DOC> {

    private final FindPublisher<DOC> result;

    public FindJsonResult(FindPublisher<DOC> result, Conversions conversions, Materializer materializer) {
        super(result, conversions, materializer);
        this.result = result;
    }

    @Override
    public CompletionStage<Option<DOC>> one() {
        return stream()
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }


    @Override
    public <T> CompletionStage<Option<T>> one(DocReader<DOC, T> reader) {
        return stream()
                .via(toObj(reader))
                .runWith(Sink.headOption(), materializer)
                .thenApply(Option::ofOptional);
    }


    public FindJsonResult filter(Bson filter) {
        return new FindJsonResult(result.filter(filter), conversions, materializer);
    }

    public FindJsonResult limit(int limit) {
        return new FindJsonResult(result.limit(limit), conversions, materializer);
    }

    public FindJsonResult skip(int skip) {
        return new FindJsonResult(result.skip(skip), conversions, materializer);
    }

    public FindJsonResult maxTime(long maxTime, TimeUnit timeUnit) {
        return new FindJsonResult(result.maxTime(maxTime, timeUnit), conversions, materializer);
    }

    public FindJsonResult maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        return new FindJsonResult(result.maxAwaitTime(maxAwaitTime, timeUnit), conversions, materializer);
    }

    public FindJsonResult modifiers(Bson modifiers) {
        return new FindJsonResult(result.modifiers(modifiers), conversions, materializer);
    }

    public FindJsonResult projection(Bson projection) {
        return new FindJsonResult(result.projection(projection), conversions, materializer);
    }

    public FindJsonResult sort(Bson sort) {
        return new FindJsonResult(result.sort(sort), conversions, materializer);
    }

    public FindJsonResult noCursorTimeout(boolean noCursorTimeout) {
        return new FindJsonResult(result.noCursorTimeout(noCursorTimeout), conversions, materializer);
    }

    public FindJsonResult oplogReplay(boolean oplogReplay) {
        return new FindJsonResult(result.oplogReplay(oplogReplay), conversions, materializer);
    }

    public FindJsonResult partial(boolean partial) {
        return new FindJsonResult(result.partial(partial), conversions, materializer);
    }

    public FindJsonResult cursorType(CursorType cursorType) {
        return new FindJsonResult(result.cursorType(cursorType), conversions, materializer);
    }

    public FindJsonResult collation(Collation collation) {
        return new FindJsonResult(result.collation(collation), conversions, materializer);
    }
}

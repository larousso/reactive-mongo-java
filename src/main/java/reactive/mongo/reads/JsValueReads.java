package reactive.mongo.reads;

import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;
import reactive.mongo.DocReader;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsValueReads<T> implements DocReader<JsValue, T> {

    final Reader<T> reader;

    private JsValueReads(Reader<T> reader) {
        this.reader = reader;
    }

    public static <R> DocReader<JsValue, R> reader(Reader<R> reader) {
        return new JsValueReads<R>(reader);
    }

    @Override
    public T read(JsValue jsValue) {
        return jsValue.as(reader);
    }
}

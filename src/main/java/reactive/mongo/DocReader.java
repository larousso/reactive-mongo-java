package reactive.mongo;

import javaslang.control.Option;

/**
 * Created by adelegue on 10/05/2017.
 */
public interface DocReader<Doc, T> {

    Option<T> read(Doc doc);

}

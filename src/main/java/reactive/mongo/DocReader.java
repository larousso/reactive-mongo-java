package reactive.mongo;

/**
 * Created by adelegue on 10/05/2017.
 */
public interface DocReader<Doc, T> {

    T read(Doc doc);

}

package reactive.mongo.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivecouchbase.json.JsValue;

/**
 * Created by adelegue on 10/05/2017.
 */
public class JsValueCodecProvider implements CodecProvider {

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if(JsValue.class.isAssignableFrom(clazz)) {
            return (Codec<T>)new JsValueCodec(registry);
        }
        return null;
    }
}

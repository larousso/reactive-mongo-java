# Mongo reactive java 

Wrapper around mongo reactive stream client with AkkaStream, Javaslang and JsonLibJavaslang support. 

## Installation 

### Repository :
 
```groovy
repositories {
    mavenCentral()
    maven {
        url 'https://raw.githubusercontent.com/larousso/reactive-mongo-java/master/repository/releases/'
    }
    flatDir {
        dirs 'libs'
    }
}
```

### Dependency : 

```groovy
dependencies {
    compile("com.adelegue:reactive-mongo-java:1.0.0")
}
```


## Usage 

```java
import static reactive.mongo.reads.JsValueReads.reader; 
import static reactive.mongo.json.MongoReads; 
import static reactive.mongo.json.MongoWrites; 
import static org.reactivecouchbase.json.Syntax.$;
```

```java 

// Reactive Mongo Java Client
ReactiveMongoClient client = ReactiveMongoClient.create(actorSystem);

//Get Database
MongoDatabase database = client.getDatabase("myDb");

//Get Collection 
MongoCollection<JsValue> collection = database.getJsonCollection("vikings");


//Insert 
JsObject ragnar = Json.obj($("name", "Ragnard"), $("childs", Json.arr($("name", "Bjorn"))));
CompletionStage<Option<Success>> insertStatus = collection.insertOne(ragnar).one();

//With mongo extended json : 
JsObject floki = Json.obj($("_id", MongoWrites.objectId(ObjectId.get())), $("name", "Floki"), $("created", MongoWrites.date(new Date())));
JsObject rollo = Json.obj($("_id", MongoWrites.objectId(ObjectId.get())), $("name", "Rollo"), $("created", MongoWrites.date(new Date())));

Future<Option<Success>> insertManyStatus = collection.insertMany(Arrays.asList(floki, rollo)).one();

//Find one 
Future<Option<JsValue>> ragnard = collection.find(Json.obj($("name", "Ragnard"))).one();

//With conversion 
Future<Option<Viking>> mayBeFloki = collection.find(Json.obj($("name", "Floki"))).one(reader(Viking.reader));

//Find multiple
Future<List<JsValue>> values = collection.find().list();

//With conversion 
Future<List<Viking>> vikings = collection.find().list(reader(Viking.reader));

//Stream 
ActorSystem actorSystem = ActorSystem.create();

Source<JsValue, NotUsed> stream = collection.find().stream();

ActorMaterializer materializer = ActorMaterializer.create(actorSystem);
stream.drop(1)
        .map(JsValue::asObject)
        .runWith(
            Sink.foreach(viking -> System.out.println(viking)), 
            materializer
        );
        
// With conversion 
Source<Viking, NotUsed> stream = collection.find().stream(reader(Viking.reader));
stream.drop(1)
        .runWith(
            Sink.foreach(viking -> System.out.println(viking)),
            materializer
        );
        
        
```





## Test 

Run mongoDB from docker before running unit tests 
```
docker run -p 27017:27017 mongo
```


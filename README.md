### Usage

**Class**
```java
public class User {
    private String name;
    private Integer favoriteNumber;
    private String favoriteColor;
    // getters and setters
}
```

**Create Schema from Class:**
```
Schema schema = AvroHelper.createSchema(User.class);
```
**Serialize Class to Avro:**
```
byte[] avroBytes = AvroHelper.toAvro(user, schema);
byte[] avroBytes = AvroHelper.toAvro(user, User.class);
```
**Serialize JSON to Avro:**
```
String json = "{\"name\":\"Thiago\",\"favoriteNumber\":777,\"favoriteColor\":\"Blue\"}";
byte[] avroBytes = AvroHelper.fromJsonToAvro(json, schema);
byte[] avroBytes = AvroHelper.fromJsonToAvro(json, User.class);
```
**Deserialize Avro to Class:**
```
User user = AvroHelper.fromAvro(avroBytes, schema);
User user = AvroHelper.fromAvro(avroBytes, User.class);
```
**Deserialize Avro to JSON:**
```
String json = AvroHelper.fromAvroToJson(avroBytes, schema);
String json = AvroHelper.fromAvroToJson(avroBytes, User.class);
```

<p><u>For more details please see the test class.</u></p>
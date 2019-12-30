package br.com.thiaguten.avro;

import org.apache.avro.Schema;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.*;

public class AvroHelperTest {

    String schemaString = """
            {
              "type" : "record",
              "name" : "User",
              "namespace" : "br.com.thiaguten.avro.AvroHelperTest",
              "fields" : [ {
                "name" : "name",
                "type" : "string"
              }, {
                "name" : "favoriteNumber",
                "type" : "int"
              }, {
                "name" : "favoriteColor",
                "type" : "string"
              } ]
            }
            """.trim();

    String allowNullSchemaString = """
            {
              "type" : "record",
              "name" : "User",
              "namespace" : "br.com.thiaguten.avro.AvroHelperTest",
              "fields" : [ {
                "name" : "name",
                "type" : [ "null", "string" ],
                "default" : null
              }, {
                "name" : "favoriteNumber",
                "type" : [ "null", "int" ],
                "default" : null
              }, {
                "name" : "favoriteColor",
                "type" : [ "null", "string" ],
                "default" : null
              } ]
            }
            """.trim();

    public static class User {

        private String name;
        private Integer favoriteNumber;
        private String favoriteColor;

        public User() {
            super();
        }

        public User(String name, Integer favoriteNumber, String favoriteColor) {
            super();
            this.name = name;
            this.favoriteNumber = favoriteNumber;
            this.favoriteColor = favoriteColor;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getFavoriteNumber() {
            return favoriteNumber;
        }

        public void setFavoriteNumber(Integer favoriteNumber) {
            this.favoriteNumber = favoriteNumber;
        }

        public String getFavoriteColor() {
            return favoriteColor;
        }

        public void setFavoriteColor(String favoriteColor) {
            this.favoriteColor = favoriteColor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            User user = (User) o;
            return Objects.equals(name, user.name) &&
                    Objects.equals(favoriteNumber, user.favoriteNumber) &&
                    Objects.equals(favoriteColor, user.favoriteColor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, favoriteNumber, favoriteColor);
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", favoriteNumber=" + favoriteNumber +
                    ", favoriteColor='" + favoriteColor + '\'' +
                    '}';
        }
    }

    @Test
    public void createSchemaWithStringTest() {
        Schema schema = AvroHelper.createSchema(schemaString);
        assertNotNull(schema);
        assertEquals(schemaString, schema.toString(true));
    }

    @Test
    public void createAllowNullSchemaWithStringTest() {
        Schema schema = AvroHelper.createSchema(allowNullSchemaString);
        assertNotNull(schema);
        assertEquals(allowNullSchemaString, schema.toString(true));
    }

    @Test
    public void createSchemaWithClassTest() {
        Schema schema = AvroHelper.createSchema(User.class);
        assertNotNull(schema);
        assertEquals(schemaString, schema.toString(true));
    }

    @Test
    public void createAllowSchemaWithClassTest() {
        Schema schema = AvroHelper.createSchema(User.class, true);
        assertNotNull(schema);
        assertEquals(allowNullSchemaString, schema.toString(true));
    }

    @Test
    public void toAvroWithClassTest() {
        User user = new User("Thiago", 31, "Blue");
        byte[] bytes = AvroHelper.toAvro(user, User.class);
        byte[] expected = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void toAvroAllowNullWithClassTest() {
        User user = new User("Thiago", null, "Blue");
        byte[] bytes = AvroHelper.toAvro(user, User.class, true);
        byte[] expected = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void toAvroWithSchemaTest() {
        Schema schema = AvroHelper.createSchema(User.class);
        assertNotNull(schema);
        User user = new User("Thiago", 31, "Blue");
        byte[] bytes = AvroHelper.toAvro(user, schema);
        byte[] expected = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void toAvroAllowNullWithSchemaTest() {
        Schema schema = AvroHelper.createSchema(User.class, true);
        assertNotNull(schema);
        User user = new User("Thiago", null, "Blue");
        byte[] bytes = AvroHelper.toAvro(user, schema);
        byte[] expected = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void fromAvroWithClassTest() {
        byte[] bytes = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        User user = AvroHelper.fromAvro(bytes, User.class);
        User expected = new User("Thiago", 31, "Blue");
        assertEquals(expected, user);
    }

    @Test
    public void fromAvroAllowNullWithClassTest() {
        byte[] bytes = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        User user = AvroHelper.fromAvro(bytes, User.class, true);
        User expected = new User("Thiago", null, "Blue");
        assertEquals(expected, user);
    }

    @Test
    public void fromAvroWithSchemaTest() {
        byte[] bytes = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        Schema schema = AvroHelper.createSchema(User.class);
        assertNotNull(schema);
        User user = AvroHelper.fromAvro(bytes, schema);
        User expected = new User("Thiago", 31, "Blue");
        assertEquals(expected, user);
    }

    @Test
    public void fromAvroAllowNullWithSchemaTest() {
        byte[] bytes = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        Schema schema = AvroHelper.createSchema(User.class, true);
        assertNotNull(schema);
        User user = AvroHelper.fromAvro(bytes, schema);
        User expected = new User("Thiago", null, "Blue");
        assertEquals(expected, user);
    }

    @Test
    public void fromJsonToAvroWithClassTest() {
        String json = "{\"name\":\"Thiago\",\"favoriteNumber\":31,\"favoriteColor\":\"Blue\"}";
        byte[] bytes = AvroHelper.fromJsonToAvro(json, User.class);
        byte[] expected = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void fromJsonToAvroAllowNullWithClassTest() {
        String json = "{\"name\":{\"string\":\"Thiago\"},\"favoriteNumber\":null,\"favoriteColor\":{\"string\":\"Blue\"}}";
        byte[] bytes = AvroHelper.fromJsonToAvro(json, User.class, true);
        byte[] expected = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void fromJsonToAvroWithSchemaTest() {
        String json = "{\"name\":\"Thiago\",\"favoriteNumber\":31,\"favoriteColor\":\"Blue\"}";
        Schema schema = AvroHelper.createSchema(User.class);
        assertNotNull(schema);
        byte[] bytes = AvroHelper.fromJsonToAvro(json, schema);
        byte[] expected = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void fromJsonToAvroAllowNullWithSchemaTest() {
        String json = "{\"name\":{\"string\":\"Thiago\"},\"favoriteNumber\":null,\"favoriteColor\":{\"string\":\"Blue\"}}";
        Schema schema = AvroHelper.createSchema(User.class, true);
        assertNotNull(schema);
        byte[] bytes = AvroHelper.fromJsonToAvro(json, schema);
        byte[] expected = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        assertArrayEquals(expected, bytes);
    }

    @Test
    public void fromAvroToJsonWithClassTest() {
        byte[] bytes = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        String json = AvroHelper.fromAvroToJson(bytes, User.class);
        String expected = "{\"name\":\"Thiago\",\"favoriteNumber\":31,\"favoriteColor\":\"Blue\"}";
        assertEquals(expected, json);
    }

    @Test
    public void fromAvroToJsonAllowNullWithClassTest() {
        byte[] bytes = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        String json = AvroHelper.fromAvroToJson(bytes, User.class, true);
        String expected = "{\"name\":{\"string\":\"Thiago\"},\"favoriteNumber\":null,\"favoriteColor\":{\"string\":\"Blue\"}}";
        assertEquals(expected, json);
    }

    @Test
    public void fromAvroToJsonWithSchemaTest() {
        byte[] bytes = {12, 84, 104, 105, 97, 103, 111, 62, 8, 66, 108, 117, 101};
        Schema schema = AvroHelper.createSchema(User.class);
        assertNotNull(schema);
        String json = AvroHelper.fromAvroToJson(bytes, schema);
        String expected = "{\"name\":\"Thiago\",\"favoriteNumber\":31,\"favoriteColor\":\"Blue\"}";
        assertEquals(expected, json);
    }

    @Test
    public void fromAvroToJsonAllowNullWithSchemaTest() {
        byte[] bytes = {2, 12, 84, 104, 105, 97, 103, 111, 0, 2, 8, 66, 108, 117, 101};
        Schema schema = AvroHelper.createSchema(User.class, true);
        assertNotNull(schema);
        String json = AvroHelper.fromAvroToJson(bytes, schema);
        String expected = "{\"name\":{\"string\":\"Thiago\"},\"favoriteNumber\":null,\"favoriteColor\":{\"string\":\"Blue\"}}";
        assertEquals(expected, json);
    }
}

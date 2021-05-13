package br.com.thiaguten.avro;

import org.apache.avro.Schema;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Objects;

import static org.junit.Assert.*;

public class AvroHelperTest {

    private final String schemaString = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"User\",\n" +
            "  \"namespace\" : \"br.com.thiaguten.avro.AvroHelperTest\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"name\",\n" +
            "    \"type\" : \"string\"\n" +
            "  }, {\n" +
            "    \"name\" : \"favoriteNumber\",\n" +
            "    \"type\" : \"int\"\n" +
            "  }, {\n" +
            "    \"name\" : \"favoriteColor\",\n" +
            "    \"type\" : \"string\"\n" +
            "  } ]\n" +
            "}";

    private final String allowNullSchemaString = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"User\",\n" +
            "  \"namespace\" : \"br.com.thiaguten.avro.AvroHelperTest\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"name\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"favoriteNumber\",\n" +
            "    \"type\" : [ \"null\", \"int\" ],\n" +
            "    \"default\" : null\n" +
            "  }, {\n" +
            "    \"name\" : \"favoriteColor\",\n" +
            "    \"type\" : [ \"null\", \"string\" ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";

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

    private File createSchemaFile(String name, String content) throws IOException {
        String dir = System.getProperty("user.dir");
        Path path = Paths.get(dir, name);
        OpenOption[] options = { StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING };
        Files.write(path, content.getBytes(StandardCharsets.UTF_8), options);
        File file = path.toFile();
        file.deleteOnExit();
        return file;
    }

    @Test
    public void createSchemaWithFileTest() throws IOException {
        File file = createSchemaFile("UserSchema.avsc", schemaString);
        Schema schema = AvroHelper.createSchema(file);
        assertNotNull(schema);
        assertEquals(schemaString, schema.toString(true));
    }

    @Test
    public void createAllowNullSchemaWithFileTest() throws IOException {
        File file = createSchemaFile("UserAllowNullSchema.avsc", allowNullSchemaString);
        Schema schema = AvroHelper.createSchema(file);
        assertNotNull(schema);
        assertEquals(allowNullSchemaString, schema.toString(true));
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

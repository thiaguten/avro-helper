package br.com.thiaguten.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.*;

/**
 * Avro utility class for serialization and deserialization.
 *
 * @author Thiago Gutenberg Carvalho da Costa
 */
public final class AvroHelper {

	private AvroHelper() {
		// not instantiable
	}

	public static <T> T fromAvro(byte[] payload, Class<T> schemaType) {
		return fromAvro(payload, schemaType, false);
	}

	public static <T> T fromAvro(byte[] payload, Class<T> schemaType, boolean allowNull) {
		Schema schema = createSchema(schemaType, allowNull);
		return fromAvro(payload, schema);
	}

	public static <T> T fromAvro(byte[] payload, Schema schema) {
		try (ByteArrayInputStream inputStream = new ByteArrayInputStream(payload)) {
			DatumReader<T> datumReader = new ReflectDatumReader<>(schema);
			BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
			T datum = null;
			while (!binaryDecoder.isEnd()) {
				datum = datumReader.read(datum, binaryDecoder);
			}
			return datum;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> byte[] toAvro(T object, Class<T> schemaType) {
		return toAvro(object, schemaType, false);
	}

	public static <T> byte[] toAvro(T object, Class<T> schemaType, boolean allowNull) {
		Schema schema = createSchema(schemaType, allowNull);
		return toAvro(object, schema);
	}

	public static <T> byte[] toAvro(T object, Schema schema) {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			DatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
			datumWriter.write(object, encoder);
			encoder.flush();
			outputStream.flush();
			return outputStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> String fromAvroToJson(byte[] avroBytes, Class<T> schemaType) {
		return fromAvroToJson(avroBytes, schemaType, false);
	}

	public static <T> String fromAvroToJson(byte[] avroBytes, Class<T> schemaType , boolean allowNull) {
		Schema schema = createSchema(schemaType, allowNull);
		return fromAvroToJson(avroBytes, schema);
	}

	public static String fromAvroToJson(byte[] avroBytes, Schema schema) {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
			DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
			BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
			JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
			Object datum = null;
			while (!binaryDecoder.isEnd()) {
				datum = datumReader.read(datum, binaryDecoder);
				datumWriter.write(datum, jsonEncoder);
				jsonEncoder.flush();
			}
			outputStream.flush();
			return outputStream.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> byte[] fromJsonToAvro(String json, Class<T> schemaType) {
		return fromJsonToAvro(json, schemaType, false);
	}

	public static <T> byte[] fromJsonToAvro(String json, Class<T> schemaType, boolean allowNull) {
		Schema schema = createSchema(schemaType, allowNull);
		return fromJsonToAvro(json, schema);
	}

	public static byte[] fromJsonToAvro(String json, Schema schema) {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
			DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
			BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
			JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, json);
			Object datum = null;
			while (true) {
				try {
					datum = datumReader.read(datum, jsonDecoder);
				} catch (EOFException eofException) {
					break;
				}
				datumWriter.write(datum, binaryEncoder);
				binaryEncoder.flush();
			}
			outputStream.flush();
			return outputStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Schema createSchema(Class<?> schemaType) {
		return createSchema(schemaType, false);
	}

	public static Schema createSchema(Class<?> schemaType, boolean allowNull) {
		return allowNull
				? ReflectData.AllowNull.get().getSchema(schemaType)
				: ReflectData.get().getSchema(schemaType);
	}

	public static Schema createSchema(String input) {
		return new Schema.Parser().parse(input);
	}

	public static Schema createSchema(File input) throws IOException {
		return new Schema.Parser().parse(input);
	}

}
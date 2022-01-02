package com.betterreads.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.betterreads.betterreadsdataloader.author.Author;
import com.betterreads.betterreadsdataloader.author.AuthorRepository;
import com.betterreads.betterreadsdataloader.book.Book;
import com.betterreads.betterreadsdataloader.book.BookRepository;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorsDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	private void initAuthors() {

		// Read the lines from the file
		Path path = Paths.get(authorsDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {

			// Parse the line into JSONObject
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject;

				try {
					// Construct the Author Object
					jsonObject = new JSONObject(jsonString);
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalname(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));

					// Save the Object in DB
					authorRepository.save(author);

				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try (Stream<String> lines = Files.lines(path)) {

			// Parse the line into JSONObject
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));

				// Create the Book Object
				try {

					// Creating a Json Object to store the parsed String
					JSONObject jsonObject = new JSONObject(jsonString);
					Book book = new Book();

					// from that object we can get the values by providing the keys in the
					// JsonObjects
					book.setId(jsonObject.getString("key").replace("/works/", ""));

					book.setName(jsonObject.optString("title"));

					// If the value is an JsonObject then store the JsonObject by creating an
					// another Obj
					JSONObject descriptionObject = jsonObject.optJSONObject("description");
					if (descriptionObject != null) {
						book.setDescription(descriptionObject.optString("value"));
					}

					// If the value is an array create a JSONArray and store the array by providing
					// the key
					JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
					if (coversJSONArr != null) {
						// Itterate over the array
						List<String> coverIds = new ArrayList<String>();
						// Convert each Id into string by using getString
						for (int i = 0; i < coversJSONArr.length(); i++) {
							coverIds.add(coversJSONArr.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorsIds = jsonObject.optJSONArray("authors");
					if (authorsIds != null) {
						List<String> authorId = new ArrayList<String>();
						for (int i = 0; i < authorsIds.length(); i++) {
							JSONObject authorObj = authorsIds.getJSONObject(i);
							// String authorIDS =
							// authorsIds.getJSONObject(i).getJSONObject("authors").getString("key")
							// .replace("/authors/", "");
							JSONObject authorIds = authorObj.optJSONObject("author");
							authorId.add(authorIds.optString("key").replace("/authors/", ""));
						}
						book.setAuthorId(authorId);

						List<String> authorNames = authorId.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unkown Author";
									else
										return optionalAuthor.get().getName();
								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}

					JSONObject publishedObject = jsonObject.optJSONObject("created");
					if (publishedObject != null) {
						String dateStr = publishedObject.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}

					// Save it to DB
					System.out.println("Saving Book " + book.getName() + "...");
					bookRepository.save(book);
				} catch (JSONException e) {
					e.printStackTrace();
				}

			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {

		initAuthors();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}

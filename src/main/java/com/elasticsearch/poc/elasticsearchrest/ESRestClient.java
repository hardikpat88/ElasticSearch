package com.elasticsearch.poc.elasticsearchrest;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.elasticsearch.poc.elasticsearchrest.model.Article;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Update;

/**
*/

public class ESRestClient {
	
	public static void main(String[] args) {
		System.out.println("Starting the ES Client...");
		try {

			
			getArticleById("AWduH09pL_j6Xws3Tc3w");
			List<Article> articles = getAllArticle();
			System.out.println("articles..."+articles);
			
			getArticle("article","technology","title","Article1");
			Article updatedArticle = new Article(222, "Article1wwe", "contentswsws1", "http://www.google.com", new Date(), "source1",
					"Hardik Patel");
			updateArticle(updatedArticle,"AWduH09pL_j6Xws3Tc3w","article","technology");
			
			//deleteArticle("AWduIFlbL_j6Xws3Tc3x","article","technology");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static JestClient getJestClient() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://127.0.0.1:9200")
				.multiThreaded(true).build());
		JestClient client = factory.getObject();
		return client;
	}

	private static void createIndex() throws IOException {
		System.out.println("Creating Index -start...");
		Article article1 = new Article(123, "Article1", "content1", "http://www.abc.com", new Date(), "source1",
				"James Bond");
		Article article2 = new Article(345, "Article2", "content2", "http://www.xyz.com", new Date(), "source2",
				"James Wang");
		Article article3 = new Article(567, "Article3", "content3", "http://www.wre.com", new Date(), "source3",
				"Peter Bond");
		Index index1 = new Index.Builder(article1).index("article").type("technology").build();
		Index index2 = new Index.Builder(article2).index("article").type("technology").build();
		Index index3 = new Index.Builder(article3).index("article").type("business").build();
		JestResult jestResult1 = getJestClient().execute(index1);
		System.out.println(jestResult1.getJsonString());
		JestResult jestResult2 = getJestClient().execute(index2);
		System.out.println(jestResult2.getJsonString());
		JestResult jestResult3 = getJestClient().execute(index3);
		System.out.println(jestResult3.getJsonString());
		System.out.println("Creating Index -end...");
	}

	private static Article getArticleById(String id) throws IOException {
		System.out.println("Get article by id -start...");
		String indexName = "article";
		String indexType = "technology";
	
		Get get = new Get.Builder(indexName, id).type(indexType).build();
		JestResult result = getJestClient().execute(get);
		Article article = result.getSourceAsObject(Article.class);
		System.out.println("Get article by id -end..."+ article);
		return article;
	}

	private static List<Article> getAllArticle() throws IOException {
		System.out.println("Get all article- start...");
		String indexName = "article";
		String indexType = "technology";
		Get get = new Get.Builder(indexName, "").type(indexType).build();
		JestResult result = getJestClient().execute(get);
		List<Article> articles = result.getSourceAsObjectList(Article.class);
		System.out.println("Get all article- end..."+articles);
		return articles;
	}

	private static void updateArticle(Article article, String id, String indexName, String type) throws Exception {
		System.out.println("Update article- start...");
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = mapper.writeValueAsString(article);
		System.out.println("jsonString--->" + jsonString);
		Update update = new Update.Builder(jsonString).index(indexName).type(type).id(id).build();
		getJestClient().execute(update);
		System.out.println("Update article-- end...");
	}

	private static void deleteArticle(String id, String indexName, String type) throws IOException {
		System.out.println("Delete article- start...");
		Delete delete = new Delete.Builder(id).index(indexName).type(type).build();
		getJestClient().execute(delete);
		System.out.println("Delete article- end...");
	}

	private static List<Article> getArticle(String indexName, String type, String field, String value) throws IOException {
		System.out.println("getArticle- start...");
		String query = "{\n" +
				"    \"query\": {\n" +
				"        \"bool\" : {\n" +
				"            \"must\" : {\n" +
				"                \"query_string\" : {\n" +
				"				 \"default_field\" :\"" + field + "\",\n" +
				"                    \"query\" :\" "+value+"\"\n" +
				"                }\n" +
				"            },\n" +
				"            \"must_not\" : {\n" +
				"            }\n" +
				"        }\n" +
				"    }\n" +
				"}";

		Search search = new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex(indexName)
                .addType(type)
                .build();

		SearchResult result = getJestClient().execute(search);
		List<Article> articles = result.getSourceAsObjectList(Article.class);
		System.out.println("getArticle- end..."+articles);
		return articles;
	}
}

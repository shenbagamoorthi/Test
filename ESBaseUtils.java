
/***
*****  This class file is for Utils for ES.
****/

package com.elasticsearch.common;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.sales.intelligence.common.IbdSIAbstractModel;

public class ESBaseUtils {

	private static ESBaseUtils instance = new ESBaseUtils();
	public static ESBaseUtils getInstance() { return instance; }

	protected Properties dbProps;
	protected ResourceBundle bundle;

	protected ESBaseUtils(){
	}


	public ResourceBundle getBundle() {
		return bundle;
	}

	public void setBundle(ResourceBundle bundle) {
		this.bundle = bundle;
	}

	// Get value From the bundle
	public String getLS(String key) {
		String val = key;
		try {
			String bval = bundle.getString(key);
			if (!isEmpty(bval))
				val = bval;
		} catch (Exception ex) {
			val = key;
		}
		return val;
	}
	// Function to check empty or null value for String
	public boolean isEmpty(String str) {
		if (str == null)
			return true;
		if (str.trim().length() == 0)
			return true;
		return false;
	}
	/*
	 * Functions Used to Write Records inside ELASTIC SEARCH
	 */
	//Function to construct JSON String From a class
	public String getJsonStringFromModel (IbdSIAbstractModel mdl) throws JsonGenerationException, JsonMappingException, IOException {
		//JACKSON Object Mapper
		ObjectMapper mapper = new ObjectMapper();
		//Object to JSON in String
		String jsonString =  mapper.writeValueAsString(mdl);

		return jsonString;
	}
	//Function to construct JSON String From a class and store the result in a file
	public void writeJsonStringFromModelToFile (IbdSIAbstractModel mdl) throws JsonGenerationException, JsonMappingException, IOException {
		//JACKSON Object Mapper
		ObjectMapper mapper = new ObjectMapper();
		//Object to JSON in file
		mapper.writeValue(new File("c:\\json\\leadfile.json"), mdl);
	}
	public void insertRecord(ElasticSearch elastic,String tableName ,String id,String JSON){
		//Always TYPE should be in lower case
		elastic.insertRecord(tableName,id,JSON);

	}
	/*
	 * Functions Used to Fetch Records by ELASTIC SEARCH
	 */
	public StringBuilder constructJsonStringFromResponse(SearchResponse response){
		SearchHit[] searchHits = response.getHits().getHits();
		StringBuilder builder = new StringBuilder();
		int length = searchHits.length;
		builder.append("[");
		for (int i = 0; i < length; i++) {
			if (i == length - 1) {
				builder.append(searchHits[i].getSourceAsString());
			} else {
				builder.append(searchHits[i].getSourceAsString());
				builder.append(",");
			}
		}
		builder.append("]");
		return builder;
	}
	public String getJsonStringFromFilter(ElasticSearch elastic,String tableName,FilterBuilder filter){

		SearchResponse response = elastic.getSearchResponse(tableName.toLowerCase(), filter);
System.out.println("response:"+response);
		String result= constructJsonStringFromResponse(response).toString();
		System.out.println("result:"+result);
		return result;
	}
	public String getJsonStringFromQuery(ElasticSearch elastic,String tableName,QueryBuilder query){

		SearchResponse response = elastic.getSearchResponse(tableName.toLowerCase(), query);
		String result= constructJsonStringFromResponse(response).toString();
		System.out.println("result:"+result);
		return result;
	}
	/*
	 * Functions Used to delete Records by ELASTIC SEARCH
	 */
	public void deleteRecordsByQuery(ElasticSearch elastic,QueryBuilder qb,String tablename){
		String TYPE = tablename.toLowerCase();
		elastic.deleteRecordsByQuery(qb, TYPE);
	}
	public void deleteRecordsById(ElasticSearch elastic,String id,String tablename){
		String TYPE = tablename.toLowerCase();
		elastic.deleteRecordsById(id, TYPE);
	}
	/*
	 * Functions Used to construct filters/Query by ELASTIC SEARCH
	 */
	public FilterBuilder getFilterBuilderFromRequest(HttpServletRequest request){
		//And Filter is used to combine different where condition, Bool Filter is used as a condition for true or false
		FilterBuilder filterBuilder = FilterBuilders.andFilter(
				   FilterBuilders.termsFilter("esEntityName","selvaganesh"),
				   FilterBuilders.termsFilter("esEntityType","0"),
				   FilterBuilders.boolFilter().must( FilterBuilders.termFilter("isAuditOn", false)),

				  FilterBuilders.existsFilter("auditXML")
				  );
		//OR filter
		FilterBuilder filterBuilder7 = FilterBuilders.orFilter(
				FilterBuilders.termsFilter("esEntit","selvaganesh"),
				   FilterBuilders.termsFilter("esEntityType","0"));

		FilterBuilder filterBuilder8 = FilterBuilders.orFilter(
				FilterBuilders.termsFilter("esEntityName","selvaganesh"),
				   FilterBuilders.termsFilter("esEntityType","0"),
				   FilterBuilders.andFilter(FilterBuilders.termsFilter("esDisplayName","sel")));
		//execution mode: could be plain, fielddata, bool, and, or, bool_nocache, and_nocache or or_nocache
		/*FilterBuilder filterBuilder0 = FilterBuilders.termsFilter("esEntityName","selvaganesh","ganeshselva","").execution("plain") ;

		FilterBuilder filterBuilder1 = FilterBuilders.andFilter(
				  FilterBuilders.boolFilter().must( FilterBuilders.termFilter("isAuditOn", "false")),
				  FilterBuilders.existsFilter("management")
				  ); */
		//Filter records based on id
		FilterBuilder filterBuilder2 =  FilterBuilders.andFilter(FilterBuilders.idsFilter()
			    .addIds("46","47"));
		//Filter records from type
		FilterBuilder filterBuilder3 =  FilterBuilders.typeFilter("my_type");

		//Filter to fetch all records in a TYPE
		FilterBuilder filterBuilder4 =  FilterBuilders.matchAllFilter();

		FilterBuilder filterBuilder5 = FilterBuilders.missingFilter("createdOn")
			    .existence(true)
			    .nullValue(true);
		//Prefix Filter
		FilterBuilder filterBuilder6 = FilterBuilders.prefixFilter("esEntityName", "sel");

		return filterBuilder7;

	}
	public QueryBuilder getQueryBuilderFromRequest(HttpServletRequest request){
		QueryBuilder query = QueryBuilders.matchQuery(
			    "esEntityName",
			    "vignesh"
			);
		QueryBuilder query1 = QueryBuilders.boolQuery()
			    .must(QueryBuilders.termQuery("esEntityName", "vignesh"))
			    .must(QueryBuilders.termQuery("esEntityType", "0"))
			    .mustNot(QueryBuilders.termQuery("esEntityCode", "hfgh"))
			    .should(QueryBuilders.termQuery("esEntityCode", "vig"));

		//Query and Filter are combine by using FilteredQuery Builder
		 QueryBuilder filterQuery =
				 QueryBuilders.filteredQuery(QueryBuilders.termQuery("test",
				 "test"),FilterBuilders.termFilter("test","test"));
		 return query;

	}
	/*
	 * Functions Used to Update Records using ELASTIC SEARCH
	 */
	public void updateRecordsById(ElasticSearch elastic,String id,String tablename){
		Map<String, String> updateObject = new HashMap<String, String>();
		updateObject.put("esEntityName", "karlMarx");
		updateObject.put("esEntityType", "007");
		elastic.getClient().prepareUpdate(ESAppConfig.getInstance().getLS("ES_INDEX"),tablename.toLowerCase(),id)
		.setDoc(updateObject).setRefresh(true).execute().actionGet();
	}

}
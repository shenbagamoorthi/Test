/**
** Sample Methods for function call.
**/
package com.elasticsearch;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.elasticsearch.common.ESBaseUtils;
import com.elasticsearch.common.ElasticSearch;
import com.elasticsearch.model.EsEntity;
import com.sales.intelligence.common.IbdSIBaseModelReader;

public class FunctionCall {
	public static void main(String[] args){
		try {

			insertRecord();

			/* Filter =true
			Query=false */
			searchRecord(true);

			updateRecord();

			/*ID =true
			Query=false*/
			deleteRecord(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/** Sample function to insert the Record.
	**/
	public static void insertRecord(){
		//Initiating CLIENT
		ElasticSearch elastic = new ElasticSearch();
		EsEntity esLeadMdl = null;
		HttpServletRequest request = null;
		esLeadMdl =IbdSIBaseModelReader.getInstance().readEsEntity(request);
		//JSON String construction from EsEntity class
		String jsonInString;
		try {
			String tableName = "test";
			String id = "1";
			jsonInString = ESBaseUtils.getInstance().getJsonStringFromModel(esLeadMdl);
			ESBaseUtils.getInstance().insertRecord(elastic,tableName,id,jsonInString) ;
			ESBaseUtils.getInstance().writeJsonStringFromModelToFile(esLeadMdl);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** Sample function to search the Record.
	**/
	public static void searchRecord(boolean isFilter) throws JsonParseException, JsonMappingException, IOException{
		//Initiating CLIENT
		ElasticSearch elastic = new ElasticSearch();
		HttpServletRequest request = null;
		String tableName = "test";
		String id = "1";
		//ELASTIC SEARCH
		FilterBuilder filterBuilder = ESBaseUtils.getInstance().getFilterBuilderFromRequest(request);
		QueryBuilder queryBuilder = ESBaseUtils.getInstance().getQueryBuilderFromRequest(request);
		if(elastic.isTypeExist(tableName)){
			if(isFilter){
				if(filterBuilder!=null ){
					EsEntity[] mdlArray = null;
					String json = ESBaseUtils.getInstance().getJsonStringFromFilter(elastic,tableName, filterBuilder);
					ObjectMapper mapper = new ObjectMapper();
					//JSON from String to Object
					mdlArray = mapper.readValue(json, EsEntity[].class);
					/*
					 * Loop for the above class Array length and set n the grid fields
					 * */
				}
			}else{
				if(queryBuilder!=null){
					EsEntity[] mdlArray = null;
					String json = ESBaseUtils.getInstance().getJsonStringFromQuery(elastic,tableName, queryBuilder);
					ObjectMapper mapper = new ObjectMapper();
					//JSON from String to Object
					mdlArray = mapper.readValue(json, EsEntity[].class);
					/*
					 * Loop for the above class Array length and set n the grid fields
					 * */
				}
			}
		}

	}
	public static void updateRecord(){
		ElasticSearch elastic = new ElasticSearch();
		String tableName = "test";
		String id = "1";
		ESBaseUtils.getInstance().updateRecordsById(elastic,id,tableName);
	}
	public static void deleteRecord(boolean isId){
		ElasticSearch elastic = new ElasticSearch();
		HttpServletRequest request = null;
		String tableName = "test";
		String id = "1";
		if(isId){
			ESBaseUtils.getInstance().deleteRecordsById(elastic,id,tableName);
		}else{
			QueryBuilder queryBuilder = ESBaseUtils.getInstance().getQueryBuilderFromRequest(request);
			ESBaseUtils.getInstance().deleteRecordsByQuery(elastic,queryBuilder,tableName);
		}

	}
}


/***
*****  This class file is used to get ES client and client functions
****/
package com.elasticsearch.common;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.NodeBuilder;

import com.revenueplus.budget.base.ui.BaseUtils;


public class ElasticSearch {
	private Client client ;

	//Constructor to create Client
	public ElasticSearch(){
		if(BaseUtils.getInstance().convertStringToBoolean(ESAppConfig.getInstance().getLS("IS_LOCAL_CLIENT"))){
			this.client=NodeBuilder.nodeBuilder()
					.client(true)
					.node()
					.client();
		}else{
			this.client= new TransportClient()
		    .addTransportAddress(new InetSocketTransportAddress(ESAppConfig.getInstance().getLS("TRANSPORT_CLIENT_NAME"), 9200));

		}

	}
	//Function to check Whether the INDEX exist in ELASTICSEARCH
	public boolean isIndexExist(){
		boolean exists = client.admin().indices().prepareExists(ESAppConfig.getInstance().getLS("ES_INDEX")).execute().actionGet().isExists();
		return exists;
	}
	//Function to check Whether the TYPE exist in INDEX/ELASTICSEARCH
	 public boolean isTypeExist(String type){
		boolean val = false;
		//Check whether INDEX is present and then check for type
		if(isIndexExist()){
			 ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet();
			 ImmutableOpenMap<String, MappingMetaData> mappings = resp.getState().metaData().index(ESAppConfig.getInstance().getLS("ES_INDEX")).mappings();
			 if (mappings.containsKey(type.toLowerCase())) {
				 return true;
			 }
		}
		 return val;
	 }
	 //Function to get the CLIENT Explicit
	public Client getClient(){
		if(BaseUtils.getInstance().convertStringToBoolean(ESAppConfig.getInstance().getLS("IS_LOCAL_CLIENT"))){
			this.client=NodeBuilder.nodeBuilder()
					.client(true)
					.node()
					.client();
		}else{
			this.client= new TransportClient()
		    .addTransportAddress(new InetSocketTransportAddress(ESAppConfig.getInstance().getLS("TRANSPORT_CLIENT_NAME"), 9200));

		}
		return client;
	}
	/*INDEX : Database Name
	 * TYPE : Table Name
	 * JSON : Column name with values in JSON format eg:{"name":"selva","age":"25","sex":"male"}
	 */
	//Function to insert record in the TYPE
	public void insertRecord(String tablename,String id,String JSON){
		//Always TYPE should be in lower case
		String TYPE = tablename.toLowerCase();
		 client.prepareIndex(ESAppConfig.getInstance().getLS("ES_INDEX"), TYPE,id)
		        .setSource(JSON)
		        .execute()
		        .actionGet();
	}
	//Function to search Records from TYPE by using filters
	public SearchResponse getSearchResponse(String tablename,FilterBuilder filter){
		//Always TYPE should be in lower case
		String TYPE = tablename.toLowerCase();
		SearchResponse response = client.prepareSearch(ESAppConfig.getInstance().getLS("ES_INDEX"))
		        .setTypes(TYPE)
		        .setPostFilter(filter)   // Filter
		        .setFrom(0).setSize(60).setExplain(true)
		        .execute()
		        .actionGet();
	return response;
	}
	//Function to search Records from TYPE by using query
	public SearchResponse getSearchResponse(String tablename,QueryBuilder query){
		//Always TYPE should be in lower case
		String TYPE = tablename.toLowerCase();
		SearchResponse response = client.prepareSearch(ESAppConfig.getInstance().getLS("ES_INDEX"))
		        .setTypes(TYPE)
		        .setQuery(query) // query
		        .setFrom(0).setSize(60).setExplain(true)
		        .execute()
		        .actionGet();
	return response;
	}
	//Delete Records from a TYPE by using query
	public void deleteRecordsByQuery(QueryBuilder queryBuilder,String TYPE){
		client.prepareDeleteByQuery(ESAppConfig.getInstance().getLS("ES_INDEX"))
        .setTypes(TYPE.toLowerCase())
        .setQuery(queryBuilder)
        .execute()
        .actionGet();
	}
	//Delete Records from a TYPE by using id
	public void deleteRecordsById(String id,String TYPE){
		DeleteResponse response =client.prepareDelete(ESAppConfig.getInstance().getLS("ES_INDEX"), TYPE.toLowerCase(), id)
		        .execute()
		        .actionGet();
	}
}

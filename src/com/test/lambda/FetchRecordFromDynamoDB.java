package com.test.lambda;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.KeyPair;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class FetchRecordFromDynamoDB implements RequestHandler<String,List<QuestionModel>>{

	@Override
	public List<QuestionModel> handleRequest(String input, Context context) {
		//context.getLogger().log("Welcome to Lambda");
		
		if(input == null) {
			context.getLogger().log("Welcome to Lambda");
			return null;
		}

		return fetchRecord(input);
		
		//return trigger(input);
		
	}
	
	private List<QuestionModel> fetchRecord(String input) {
		
		int index = input.indexOf(">");
		
		String tableName = input.substring(0, index);
		String chapterName = input.substring(index+1);
		
		AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient();
        Region usWest2 = Region.getRegion(Regions.AP_SOUTH_1);
        dynamoDB.setRegion(usWest2);
        
        Integer count = Integer.parseInt(System.getenv(chapterName));
		
		DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
		
		List<QuestionModel> questionModels = new ArrayList<QuestionModel>();
		

		List<KeyPair> keyPairList = new ArrayList<>();
	    
		Set<String> key = new HashSet<String>();
		
		for( int i = 0; i < 5 ; i++) {
			 KeyPair keyPair1 = new KeyPair();
			 String next = chapterName+"_"+new Random().nextInt(count);
			 if(key.contains(next)) {
				 i--;
				 continue;
			 }
			 key.add(next);
			 keyPair1.withHashKey(next);
			 keyPairList.add(keyPair1);
		}
		
		Map<Class<?>, List<KeyPair>> keyPairForTable = new HashMap<>();
	    keyPairForTable.put(QuestionModel.class, keyPairList);

	    Map<String, List<Object>> batchResults = mapper.batchLoad(keyPairForTable, 
	    		new DynamoDBMapperConfig(new DynamoDBMapperConfig.TableNameOverride(tableName)));
		
	   // System.out.println("->"+batchResults.values().iterator().next());
	   //Collection<java.util.List<java.lang.Object>>
	    //questionModels.addAll((Collection<java.util.List<? extends QuestionModel>>) batchResults.values());
	    
	    for(Object qModel : batchResults.values().iterator().next()) {
	    	questionModels.add((QuestionModel)qModel);
	    }
		
		return questionModels;
	}

}

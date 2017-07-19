package com.test.lambda;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class FetchRecordFromDynamoDB implements RequestHandler<String,String>{

	@Override
	public String handleRequest(String input, Context context) {
		context.getLogger().log("Welcome to Lambda");
		
		trigger(input);
		
		return null;
	}

	private void trigger(String input) {
		Integer count = Integer.parseInt(System.getenv(input));
		
		AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient();
        Region usWest2 = Region.getRegion(Regions.US_EAST_1);
        dynamoDB.setRegion(usWest2);
        
        Map<String,String> expressionAttributesNames = new HashMap<>();
	    expressionAttributesNames.put("#questionId","questionId");
	    
	    Map<String,AttributeValue> expressionAttributeValues = new HashMap<>();
	    expressionAttributeValues.put(":questionValue",new AttributeValue().withN(""+new Random().nextInt(count)));
		
		QueryRequest queryRequest = new QueryRequest()
		        .withTableName(input)
		        .withKeyConditionExpression("#questionId = :questionValue")
		        .withExpressionAttributeNames(expressionAttributesNames)
		        .withExpressionAttributeValues(expressionAttributeValues);
		
		List<Map<String,AttributeValue>> attributeValues = dynamoDB.query(queryRequest).getItems();
		
		System.out.println(attributeValues.get(0));
	}
	
	public static void main(String a[]) {
		//System.getenv().put("profit_loss", "181");
		FetchRecordFromDynamoDB test = new FetchRecordFromDynamoDB();
		test.trigger("profit_loss");
	}
	
	
	

}

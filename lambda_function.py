import json
import boto3
import json
def lambda_handler(event, context):
    print("hiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
    dynamobd=boto3.client("dynamodb")
    
    if "First_Name" in event  or "Last_Name" in event or "Date_Of_Joining" in event:
        Emp_Id=event.get("Emp_Id","None")
        First_Name=event.get("First_Name","None")
        Last_Name=event.get("Last_Name","None")
        Date_Of_Joining=event.get("Date_Of_Joining","None")
        data={
        'Emp_Id': {
            'S': Emp_Id,
        },
        'First_Name': {
            'S': First_Name,
        },
        'Last_Name': {
            'S': Last_Name,
        },
        'Date_Of_Joining': {
            'S': Date_Of_Joining,
        },}
        response = dynamobd.put_item(
        Item=data,
        ReturnConsumedCapacity='TOTAL',
        TableName='Emp_Master',)

        return {
            'statusCode': 200,
            'body': json.dumps("Data sucessfully added")
        }
    elif "Emp_Id" in event:
        Emp_Id=event.get("Emp_Id")
        print(Emp_Id,event)
        response = dynamobd.get_item(
        Key={
            'Emp_Id': {
                'S': Emp_Id,
            },
    },
    TableName='Emp_Master',
)   
        return {
        'statusCode': 200,
        'body': json.dumps(response)
        }
    else:
        return {
        'statusCode': 400,
        'body': json.dumps("Please provide valid input")
        }
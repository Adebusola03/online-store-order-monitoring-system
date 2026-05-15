import json
import boto3
import os
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Products')

sns = boto3.client('sns')
sns_topic = os.environ['SNS_TOPIC_ARN']

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError

def response(status, body):
    return {
        'statusCode': status,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
        },
        'body': json.dumps(body, default=decimal_default)
    }

def lambda_handler(event, context):
    try:
        method = event.get("requestContext", {}).get("http", {}).get("method")
        path = event.get("rawPath", "")
        path_params = event.get("pathParameters") or {}
        body = json.loads(event["body"]) if event.get("body") else {}

        # POST /order
        if method == "POST" and path.endswith("/order"):
            product_id = body["productId"]
            quantity = int(body["quantity"])

            result = table.get_item(Key={"productId": product_id})
            if "Item" not in result:
                return response(404, {"message": "Product not found"})

            product = result["Item"]
            current_stock = int(product["stock"])
            threshold = int(product["threshold"])

            if quantity > current_stock:
                return response(400, {"message": "Not enough stock available"})

            new_stock = current_stock - quantity

            table.update_item(
                Key={"productId": product_id},
                UpdateExpression="SET stock = :s",
                ExpressionAttributeValues={":s": new_stock}
            )

            if new_stock <= threshold:
                sns.publish(
                    TopicArn=sns_topic,
                    Subject="Low Inventory Alert",
                    Message=f"Low stock alert for {product['productName']}. Remaining stock: {new_stock}"
                )

            return response(200, {
                "message": "Order processed successfully",
                "remainingStock": new_stock
            })

        # GET /products
        if method == "GET" and path.endswith("/products"):
            result = table.scan()
            return response(200, result.get("Items", []))

        # GET /products/{id}
        if method == "GET" and "id" in path_params:
            product_id = path_params["id"]
            result = table.get_item(Key={"productId": product_id})
            if "Item" not in result:
                return response(404, {"message": "Product not found"})
            return response(200, result["Item"])

        # POST /products
        if method == "POST" and path.endswith("/products"):
            item = {
                "productId": body["productId"],
                "productName": body["productName"],
                "stock": int(body["stock"]),
                "threshold": int(body["threshold"])
            }
            table.put_item(Item=item)
            return response(201, {"message": "Product added successfully"})

        # PUT /products/{id}
        if method == "PUT" and "id" in path_params:
            product_id = path_params["id"]
            table.update_item(
                Key={"productId": product_id},
                UpdateExpression="SET productName = :n, stock = :s, threshold = :t",
                ExpressionAttributeValues={
                    ":n": body["productName"],
                    ":s": int(body["stock"]),
                    ":t": int(body["threshold"])
                }
            )
            return response(200, {"message": "Product updated successfully"})

        # DELETE /products/{id}
        if method == "DELETE" and "id" in path_params:
            product_id = path_params["id"]
            table.delete_item(Key={"productId": product_id})
            return response(200, {"message": "Product deleted successfully"})

        return response(400, {"message": "Unsupported route or method"})

    except Exception as e:
        return response(500, {"message": "Internal server error", "error": str(e)})
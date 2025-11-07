import json
import os
import boto3

sf = boto3.client("stepfunctions")
STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN", "arn:aws:states:us-east-1:933000400558:stateMachine:DriftReportAgentASL")

def lambda_handler(event, context):
    # Parse input body từ API Gateway
    # ✅ Đồng bộ dữ liệu
    input_data = {
        "query": f"Phân tích drift từ log CICD: {event['body']}",
        "type": "cicd_log"
    }
    print(input_data)


    # Kiểm tra biến môi trường
    if not STEP_FUNCTION_ARN:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "STEP_FUNCTION_ARN is not configured"})
        }

    # 🚀 Start Step Function
    execution = sf.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps(input_data)
    )

    execution_arn = execution["executionArn"]

    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "PROCESSING_STARTED",
            "execution_arn": execution_arn,
        }, ensure_ascii=False)
    }

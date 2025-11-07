import boto3
import os
import json
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime, timezone
import re

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("repoSubscriptions")
sf = boto3.client("stepfunctions")
STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN", "arn:aws:states:us-east-1:933000400558:stateMachine:DriftReportAgentASL")
lambda_client = boto3.client("lambda")
LAMBDA_NAME = os.environ.get("LAMBDA_NAME", "iacScanOrchestrator")


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def extract_repo_url(text: str):
    m = re.search(r"https?://github\.com/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+", text)
    return m.group(0) if m else None

# --- Handler 1: Reset Weekly ---
def reset_scan_status():
    print("🔄 Reset scanStatus to PENDING")
    response = table.scan(
        FilterExpression=Attr("active").eq("ACTIVE")
    )
    for item in response.get("Items", []):
        table.update_item(
            Key={"repoUrl": item["repoUrl"]},
            UpdateExpression="SET scanStatus = :p, updatedAt = :t",
            ExpressionAttributeValues={":p": "PENDING", ":t": now_utc()},
        )
    print("✅ Reset done. Starting first scan...")
    lambda_client.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps({"eventName": "scan"})
    )
    return {"status": "reset_done"}


# --- Handler 2: Scan Next Repo ---
def scan_next_repo():
    response = table.query(
        IndexName='active-scanStatus-index',
        KeyConditionExpression=Key('active').eq('ACTIVE') & Key('scanStatus').eq('PENDING')
    )
    items = response.get("Items", [])
    if not items:
        print("✅ No more repos to scan.")
        return {"status": "done"}

    repo = items[0]
    repo_url = repo["repoUrl"]

    # mark in-progress
    table.update_item(
        Key={"repoUrl": repo_url},
        UpdateExpression="SET scanStatus = :s, updatedAt = :t",
        ExpressionAttributeValues={":s": "IN_PROGRESS", ":t": now_utc()}
    )

    payload = {
        "query": f"Hãy so sánh drift toàn bộ resource trong repo {repo_url}",
        "type": "full_scan"
    }

    sf.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps(payload)
    )

    print(f"🚀 Step Function triggered for: {repo_url}")
    return {"status": "started", "repo": repo_url}


# --- Master Handler ---
def lambda_handler(event, context):
    event_name = event.get("eventName")
    query = event.get("query", "").strip()
    repo_url = extract_repo_url(query)
    print(f"🔍 Event: {event_name}, Query: {query}, Repo: {repo_url}")
    
    event["repoUrl"] = repo_url

    if event_name == "reset":
        return reset_scan_status()

    if event_name == "scan":
        return scan_next_repo()

    return {"error": f"Unknown eventName: {event_name}"}

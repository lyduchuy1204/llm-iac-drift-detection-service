import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Tự động trigger Config snapshot khi có configuration changes
    """
    config_client = boto3.client('config')
    
    try:
        # Parse EventBridge event
        detail = event.get('detail', {})
        resource_type = detail.get('resourceType', 'Unknown')
        resource_id = detail.get('resourceId', 'Unknown')
        config_item_status = detail.get('configurationItemStatus', 'Unknown')
        
        logger.info(f"Configuration change detected:")
        logger.info(f"Resource Type: {resource_type}")
        logger.info(f"Resource ID: {resource_id}")
        logger.info(f"Status: {config_item_status}")
        
        # Trigger configuration snapshot
        response = config_client.deliver_config_snapshot(
            deliveryChannelName='default'
        )
        
        snapshot_id = response['configSnapshotId']
        logger.info(f"Successfully triggered snapshot: {snapshot_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Snapshot triggered successfully',
                'snapshotId': snapshot_id,
                'resourceType': resource_type,
                'resourceId': resource_id
            })
        }
        
    except Exception as e:
        logger.error(f"Error triggering snapshot: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

import json, uuid, os, math
from collections import defaultdict
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta

BUCKET     = os.environ['BUCKET_NAME']
TABLE      = os.environ['SIGHTINGS_TABLE']
CENTER_LAT = float(os.environ['CENTER_LAT'])
CENTER_LON = float(os.environ['CENTER_LON'])
GRID_KM    = 4.0

def snap_to_grid(lat, lon):
    lat_per_km = 1.0 / 111.0
    lon_per_km = 1.0 / (111.0 * math.cos(math.radians(CENTER_LAT)))
    i = round((lat - CENTER_LAT) / (GRID_KM * lat_per_km))
    j = round((lon - CENTER_LON) / (GRID_KM * lon_per_km))
    return (
        round(CENTER_LAT + i * GRID_KM * lat_per_km, 5),
        round(CENTER_LON + j * GRID_KM * lon_per_km, 5),
    )

def lambda_handler(event, context):
    try:
        body      = json.loads(event.get('body') or '{}')
        lat       = float(body['lat'])
        lon       = float(body['lon'])
        device_id = str(body.get('device_id', ''))[:64]
    except (KeyError, ValueError, TypeError):
        return {'statusCode': 400, 'body': 'lat/lon required'}

    lat, lon = snap_to_grid(lat, lon)
    now = datetime.now(timezone.utc)

    boto3.resource('dynamodb').Table(TABLE).put_item(Item={
        'id':        str(uuid.uuid4()),
        'lat':       str(lat),
        'lon':       str(lon),
        'device_id': device_id,
        'timestamp': now.isoformat(),
        'ttl':       int((now + timedelta(days=7)).timestamp()),
    })

    _refresh_sightings_json()

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'ok': True, 'lat': lat, 'lon': lon}),
    }

def _refresh_sightings_json():
    table  = boto3.resource('dynamodb').Table(TABLE)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    resp   = table.scan(FilterExpression=Attr('timestamp').gte(cutoff))

    groups = defaultdict(list)
    for item in resp.get('Items', []):
        key = (item['lat'], item['lon'])
        groups[key].append(item['timestamp'])

    features = [{
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [float(lon), float(lat)]},
        'properties': {
            'count':          len(timestamps),
            'last_timestamp': max(timestamps),
        },
    } for (lat, lon), timestamps in groups.items()]

    boto3.client('s3').put_object(
        Bucket=BUCKET,
        Key='sightings.json',
        Body=json.dumps({'type': 'FeatureCollection', 'features': features}).encode('utf-8'),
        ContentType='application/json',
        CacheControl='max-age=60',
    )

"""
Rainbow Forecast Lambda — 沖縄版
那覇中心 100km 圏内を 4km グリッドで走査し、虹スコアを S3 に保存。
スコア 70+ のエリアが存在する場合、近隣デバイスに push 通知を送信する。
"""
import json, math, urllib.request, os
from collections import defaultdict
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────
CENTER_LAT   = 26.45
CENTER_LON   = 127.90
SIDE_KM      = 100
GRID_KM      = 4
WEATHER_KM   = 10
HALF_N       = SIDE_KM // GRID_KM    // 2
HALF_W       = SIDE_KM // WEATHER_KM // 2
COVERAGE_KM  = SIDE_KM / 2

BUCKET           = os.environ['BUCKET_NAME']
KEY              = 'latest.geojson'
DEVICES_TABLE    = os.environ.get('DEVICES_TABLE', '')
SIGHTINGS_TABLE  = os.environ.get('SIGHTINGS_TABLE', '')
ARN_SANDBOX      = os.environ.get('SNS_PLATFORM_ARN_SANDBOX', '')
ARN_PROD         = os.environ.get('SNS_PLATFORM_ARN_PROD', '')
REGION_ID        = os.environ.get('REGION_ID', 'okinawa')

CARD16_JA = ['北','北北東','北東','東北東','東','東南東','南東','南南東',
              '南','南南西','南西','西南西','西','西北西','北西','北北西']

# ── Grid generation ───────────────────────────────────────────
def generate_grid():
    lat_per_km = 1.0 / 111.0
    lon_per_km = 1.0 / (111.0 * math.cos(math.radians(CENTER_LAT)))
    points = []
    for i in range(-HALF_N, HALF_N + 1):
        for j in range(-HALF_N, HALF_N + 1):
            lat = CENTER_LAT + i * GRID_KM * lat_per_km
            lon = CENTER_LON + j * GRID_KM * lon_per_km
            points.append((round(lat, 5), round(lon, 5)))
    return points

# ── Solar position (NOAA 方式) ────────────────────────────────
def sun_position(utc_dt, lat_deg, lon_deg):
    jd = utc_dt.timestamp() / 86400.0 + 2440587.5
    jc = (jd - 2451545.0) / 36525.0

    L0 = (280.46646 + jc * (36000.76983 + jc * 0.0003032)) % 360
    M  = 357.52911 + jc * (35999.05029 - 0.0001537 * jc)
    e  = 0.016708634 - jc * (0.000042037 + 0.0000001267 * jc)
    M_r = math.radians(M)

    C = (math.sin(M_r)     * (1.914602 - jc * (0.004817 + 0.000014 * jc))
       + math.sin(2 * M_r) * (0.019993 - 0.000101 * jc)
       + math.sin(3 * M_r) * 0.000289)

    omega     = 125.04 - 1934.136 * jc
    app_lon_r = math.radians(L0 + C - 0.00569 - 0.00478 * math.sin(math.radians(omega)))

    obliq_deg = (23.0
        + (26.0 + (21.448 - jc * (46.815 + jc * (0.00059 - jc * 0.001813))) / 60.0) / 60.0
        + 0.00256 * math.cos(math.radians(omega)))
    obliq_r = math.radians(obliq_deg)
    dec     = math.asin(math.sin(obliq_r) * math.sin(app_lon_r))

    L0_r = math.radians(L0)
    y    = math.tan(obliq_r / 2.0) ** 2
    eot  = 4.0 * math.degrees(
          y      * math.sin(2 * L0_r)
        - 2 * e  * math.sin(M_r)
        + 4 * e * y * math.sin(M_r) * math.cos(2 * L0_r)
        - 0.5 * y * y * math.sin(4 * L0_r)
        - 1.25 * e * e * math.sin(2 * M_r)
    )

    utc_min = utc_dt.hour * 60 + utc_dt.minute + utc_dt.second / 60.0
    tst  = (utc_min + lon_deg * 4.0 + eot) % 1440.0
    ha   = tst / 4.0 - 180.0
    ha_r = math.radians(ha)
    lat_r = math.radians(lat_deg)

    sin_alt = max(-1.0, min(1.0,
        math.sin(lat_r) * math.sin(dec) + math.cos(lat_r) * math.cos(dec) * math.cos(ha_r)
    ))
    alt_deg = math.degrees(math.asin(sin_alt))

    cos_z = math.cos(math.asin(sin_alt))
    if abs(cos_z) < 1e-9:
        return alt_deg, 0.0
    cos_az = max(-1.0, min(1.0,
        (math.sin(dec) - math.sin(lat_r) * sin_alt) / (math.cos(lat_r) * cos_z)
    ))
    az_deg = math.degrees(math.acos(cos_az))
    if ha > 0:
        az_deg = 360.0 - az_deg

    return alt_deg, az_deg

# ── 方向オフセット座標 ────────────────────────────────────────
def offset_coordinate(lat, lon, azimuth_deg, distance_km):
    R  = 6371.0
    d  = distance_km / R
    az = math.radians(azimuth_deg)
    lat1, lon1 = math.radians(lat), math.radians(lon)
    lat2 = math.asin(math.sin(lat1) * math.cos(d) + math.cos(lat1) * math.sin(d) * math.cos(az))
    lon2 = lon1 + math.atan2(math.sin(az) * math.sin(d) * math.cos(lat1),
                              math.cos(d) - math.sin(lat1) * math.sin(lat2))
    return math.degrees(lat2), math.degrees(lon2)

def has_directional_rain(weather_map, lat, lon, anti_solar_az, distances=(3, 5, 8)):
    for dist in distances:
        tlat, tlon = offset_coordinate(lat, lon, anti_solar_az, dist)
        w = lookup_weather(weather_map, tlat, tlon)
        if w['precipitation'] > 0.1:
            return True
    return False

# ── Rainbow score ─────────────────────────────────────────────
def rainbow_score(sun_alt, cloud_cover, local_precip, directional_rain):
    if not (0 <= sun_alt <= 42): return 0
    if cloud_cover >= 70:        return 0
    if local_precip > 0.1:       return 0
    if not directional_rain:     return 0
    score = 100.0 - max(0.0, cloud_cover - 30) * 1.5
    return round(max(0.0, min(100.0, score)))

# ── Open-Meteo 天気取得 ───────────────────────────────────────
def fetch_weather_map():
    lat_per_km = 1.0 / 111.0
    lon_per_km = 1.0 / (111.0 * math.cos(math.radians(CENTER_LAT)))
    sample_points = []
    for i in range(-HALF_W, HALF_W + 1):
        for j in range(-HALF_W, HALF_W + 1):
            lat = CENTER_LAT + i * WEATHER_KM * lat_per_km
            lon = CENTER_LON + j * WEATHER_KM * lon_per_km
            sample_points.append(((i, j), round(lat, 5), round(lon, 5)))

    lats = ','.join(str(p[1]) for p in sample_points)
    lons = ','.join(str(p[2]) for p in sample_points)
    url  = (
        'https://api.open-meteo.com/v1/forecast'
        f'?latitude={lats}&longitude={lons}'
        '&current=cloud_cover,precipitation&timezone=UTC'
    )
    req = urllib.request.Request(url, headers={'User-Agent': 'rainbow-forecast/1.0'})
    with urllib.request.urlopen(req, timeout=25) as resp:
        data = json.loads(resp.read())
    if isinstance(data, dict):
        data = [data]

    weather_map = {}
    for (idx, _, _), item in zip(sample_points, data):
        c = item['current']
        weather_map[idx] = {
            'cloud_cover':   c.get('cloud_cover', 0),
            'precipitation': c.get('precipitation', 0.0),
        }
    return weather_map

def lookup_weather(weather_map, lat, lon):
    lat_per_km = 1.0 / 111.0
    lon_per_km = 1.0 / (111.0 * math.cos(math.radians(CENTER_LAT)))
    di = (lat - CENTER_LAT) / (WEATHER_KM * lat_per_km)
    dj = (lon - CENTER_LON) / (WEATHER_KM * lon_per_km)
    i  = max(-HALF_W, min(HALF_W, round(di)))
    j  = max(-HALF_W, min(HALF_W, round(dj)))
    return weather_map[(i, j)]

# ── Push 通知 ─────────────────────────────────────────────────
def notify_nearby_devices(max_score, sun_az):
    if max_score < 70 or not DEVICES_TABLE:
        return 0

    rainbow_dir = CARD16_JA[round(((sun_az + 180) % 360) / 22.5) % 16]
    title = '🌈 虹が出そうです！'
    body  = f'スコア {max_score} — {rainbow_dir}の方角を見てみて'

    table    = boto3.resource('dynamodb').Table(DEVICES_TABLE)
    response = table.scan(FilterExpression=Attr('region').eq(REGION_ID))
    sns      = boto3.client('sns')
    sent     = 0

    for item in response.get('Items', []):
        apns_payload = json.dumps({
            'aps': {
                'alert': {'title': title, 'body': body},
                'sound': 'default',
            }
        })
        message = json.dumps({
            'APNS':         apns_payload,
            'APNS_SANDBOX': apns_payload,
        })
        try:
            sns.publish(
                TargetArn=item['endpoint_arn'],
                Message=message,
                MessageStructure='json',
            )
            sent += 1
        except Exception as e:
            print(f'[PUSH ERROR] {e}')

    print(f'[PUSH] sent={sent} max_score={max_score} dir={rainbow_dir}')
    return sent

# ── Lambda エントリーポイント ─────────────────────────────────
def lambda_handler(event, context):
    now    = datetime.now(timezone.utc)
    points = generate_grid()
    sun_alt, sun_az = sun_position(now, CENTER_LAT, CENTER_LON)

    in_rain_window    = -5 <= sun_alt <= 60
    in_rainbow_window =  0 <= sun_alt <= 42

    if not in_rain_window:
        reason = 'nighttime' if sun_alt < -5 else 'sun_too_high'
        payload = {
            'type': 'FeatureCollection',
            'generated_at': now.isoformat(),
            'sun_alt': round(sun_alt, 1),
            'sun_az':  round(sun_az, 1),
            'skip_reason': reason,
            'features': [],
        }
        _upload(payload)
        _refresh_sightings_json()
        print(f'[SKIP] {reason} sun_alt={sun_alt:.1f}°')
        return {'statusCode': 200, 'body': reason}

    anti_solar  = (sun_az + 180) % 360
    weather_map = fetch_weather_map()
    features    = []
    for (lat, lon) in points:
        w = lookup_weather(weather_map, lat, lon)
        if in_rainbow_window:
            rain  = has_directional_rain(weather_map, lat, lon, anti_solar)
            score = rainbow_score(sun_alt, w['cloud_cover'], w['precipitation'], rain)
        else:
            rain  = False
            score = 0
        features.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
            'properties': {
                'score':            score,
                'sun_alt':          round(sun_alt, 1),
                'sun_az':           round(sun_az, 1),
                'cloud_cover':      w['cloud_cover'],
                'precip':           round(w['precipitation'], 2),
                'directional_rain': rain,
            },
        })

    payload = {
        'type':         'FeatureCollection',
        'generated_at': now.isoformat(),
        'sun_alt':      round(sun_alt, 1),
        'sun_az':       round(sun_az, 1),
        'rain_only':    not in_rainbow_window,
        'features':     features,
    }
    _upload(payload)

    if in_rainbow_window:
        max_score = max((f['properties']['score'] for f in features), default=0)
        notify_nearby_devices(max_score, sun_az)
        print(f'[OK] {now.isoformat()} points={len(features)} max_score={max_score} sun_alt={sun_alt:.1f}°')
    else:
        print(f'[RAIN] {now.isoformat()} points={len(features)} sun_alt={sun_alt:.1f}° (rain display only)')

    _refresh_sightings_json()
    return {'statusCode': 200, 'body': f'Processed {len(features)} points'}

def _refresh_sightings_json():
    if not SIGHTINGS_TABLE:
        return
    table  = boto3.resource('dynamodb').Table(SIGHTINGS_TABLE)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    resp   = table.scan(FilterExpression=Attr('timestamp').gte(cutoff))
    groups = defaultdict(list)
    for item in resp.get('Items', []):
        key = (item['lat'], item['lon'])
        groups[key].append(item['timestamp'])
    features = [{
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [float(lon), float(lat)]},
        'properties': {'count': len(ts), 'last_timestamp': max(ts)},
    } for (lat, lon), ts in groups.items()]
    boto3.client('s3').put_object(
        Bucket=BUCKET, Key='sightings.json',
        Body=json.dumps({'type': 'FeatureCollection', 'features': features}).encode('utf-8'),
        ContentType='application/json', CacheControl='max-age=60',
    )

def _upload(payload):
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    boto3.client('s3').put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=body,
        ContentType='application/json',
        CacheControl='max-age=540',
    )

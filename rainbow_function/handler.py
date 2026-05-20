"""
Rainbow Forecast Lambda — 沖縄版
那覇中心 100km 圏内を 4km グリッドで走査し、虹スコアを S3 に保存。
スコア 70+ のエリアが存在する場合、近隣デバイスに push 通知を送信する。
"""
import json, math, urllib.request, os, struct, zlib
from collections import defaultdict
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────
CENTER_LAT   = 26.45
CENTER_LON   = 127.75
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

def has_directional_rain(weather_map, lat, lon, anti_solar_az, distances=(10,), jma_map=None):
    for dist in distances:
        tlat, tlon = offset_coordinate(lat, lon, anti_solar_az, dist)
        precip = _get_precip_at(tlat, tlon, weather_map, jma_map)
        if precip > 0.1:
            return True
    return False

def _get_precip_at(lat, lon, weather_map, jma_map):
    if jma_map is not None:
        lat_per_km = 1.0 / 111.0
        lon_per_km = 1.0 / (111.0 * math.cos(math.radians(CENTER_LAT)))
        di = (lat - CENTER_LAT) / (WEATHER_KM * lat_per_km)
        dj = (lon - CENTER_LON) / (WEATHER_KM * lon_per_km)
        i  = max(-HALF_W, min(HALF_W, round(di)))
        j  = max(-HALF_W, min(HALF_W, round(dj)))
        if (i, j) in jma_map:
            return jma_map[(i, j)]
    return lookup_weather(weather_map, lat, lon)['precipitation']

# ── Rainbow score ─────────────────────────────────────────────
def rainbow_score(sun_alt, cloud_cover, local_precip, directional_rain, solar_cloud=0):
    if not (0 <= sun_alt <= 42): return 0
    if cloud_cover >= 40:        return 0
    if solar_cloud >= 65:        return 0
    if local_precip > 0.0:       return 0
    if not directional_rain:     return 0
    score = 100.0 - max(0.0, cloud_cover - 5) * 3.0
    return round(max(0.0, min(100.0, score)))

# ── JMA 高解像度降水ナウキャスト ──────────────────────────────
JMA_ZOOM      = 10
JMA_TIMES_URL = 'https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N1.json'
JMA_TILE_URL  = 'https://www.jma.go.jp/bosai/jmatile/data/nowc/{bt}/none/{vt}/surf/hrpns/{z}/{x}/{y}.png'
JMA_HEADERS   = {
    'User-Agent': 'Mozilla/5.0 (compatible; rainbow-forecast/1.0)',
    'Referer':    'https://www.jma.go.jp/bosai/nowc/',
}
# パレットインデックス → mm/h（JMA hrpns 10色パレット）
_JMA_PRECIP   = [0.0, 0.0, 0.1, 0.5, 2.0, 5.0, 10.0, 20.0, 30.0, 50.0]

def _latlon_to_tile_px(lat, lon, z):
    n   = 1 << z
    x_f = (lon + 180.0) / 360.0 * n
    lr  = math.radians(lat)
    y_f = (1.0 - math.log(math.tan(lr) + 1.0 / math.cos(lr)) / math.pi) / 2.0 * n
    tx, ty = int(x_f), int(y_f)
    return tx, ty, int((x_f - tx) * 256), int((y_f - ty) * 256)

def _parse_jma_tile(data):
    pos = 8
    width = height = 0
    trans = []
    idat  = []
    while pos < len(data):
        length = struct.unpack('>I', data[pos:pos+4])[0]
        ct = data[pos+4:pos+8]
        cd = data[pos+8:pos+8+length]
        pos += 12 + length
        if ct == b'IHDR':
            width, height = struct.unpack('>II', cd[:8])
        elif ct == b'tRNS':
            trans = list(cd)
        elif ct == b'IDAT':
            idat.append(cd)
        elif ct == b'IEND':
            break
    raw      = zlib.decompress(b''.join(idat))
    row_bytes = (width + 1) // 2   # 4bit: 2px/byte
    prev = bytearray(row_bytes)
    rows = []
    for row in range(height):
        off  = row * (row_bytes + 1)
        ft   = raw[off]
        line = bytearray(raw[off+1:off+1+row_bytes])
        if ft == 0:
            cur = line
        elif ft == 1:
            cur = bytearray(row_bytes)
            for i in range(row_bytes):
                cur[i] = (line[i] + (cur[i-1] if i > 0 else 0)) & 0xFF
        elif ft == 2:
            cur = bytearray((line[i] + prev[i]) & 0xFF for i in range(row_bytes))
        elif ft == 3:
            cur = bytearray(row_bytes)
            for i in range(row_bytes):
                a = cur[i-1] if i > 0 else 0
                cur[i] = (line[i] + (a + prev[i]) // 2) & 0xFF
        elif ft == 4:
            cur = bytearray(row_bytes)
            for i in range(row_bytes):
                a = cur[i-1] if i > 0 else 0
                b = prev[i]; c = prev[i-1] if i > 0 else 0
                p = a + b - c; pa, pb, pc = abs(p-a), abs(p-b), abs(p-c)
                pr = a if pa<=pb and pa<=pc else (b if pb<=pc else c)
                cur[i] = (line[i] + pr) & 0xFF
        else:
            cur = line
        rows.append(cur)
        prev = cur
    return rows, width, height, trans

def _jma_idx(rows, px, py):
    b = rows[py][px // 2]
    return (b >> 4) if (px % 2 == 0) else (b & 0x0F)

def fetch_jma_precip_map():
    """JMAナウキャストから {(i,j): mm/h} を返す。"""
    req = urllib.request.Request(JMA_TIMES_URL, headers=JMA_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as r:
        times = json.loads(r.read())
    bt = times[0]['basetime']
    vt = times[0]['validtime']

    lat_per_km = 1.0 / 111.0
    lon_per_km = 1.0 / (111.0 * math.cos(math.radians(CENTER_LAT)))
    tile_cache = {}
    precip_map = {}
    for i in range(-HALF_W, HALF_W + 1):
        for j in range(-HALF_W, HALF_W + 1):
            lat = CENTER_LAT + i * WEATHER_KM * lat_per_km
            lon = CENTER_LON + j * WEATHER_KM * lon_per_km
            tx, ty, px, py = _latlon_to_tile_px(lat, lon, JMA_ZOOM)
            if (tx, ty) not in tile_cache:
                url = JMA_TILE_URL.format(bt=bt, vt=vt, z=JMA_ZOOM, x=tx, y=ty)
                try:
                    req = urllib.request.Request(url, headers=JMA_HEADERS)
                    with urllib.request.urlopen(req, timeout=10) as r:
                        raw = r.read()
                    tile_cache[(tx, ty)] = _parse_jma_tile(raw) if raw[:4] == b'\x89PNG' else None
                except Exception as e:
                    print(f'[JMA] tile ({tx},{ty}) err: {e}')
                    tile_cache[(tx, ty)] = None
            parsed = tile_cache.get((tx, ty))
            if parsed:
                rows, w, h, trans = parsed
                if 0 <= px < w and 0 <= py < h:
                    idx = _jma_idx(rows, px, py)
                    if idx < len(trans) and trans[idx] == 0:
                        precip_map[(i, j)] = 0.0
                    else:
                        precip_map[(i, j)] = _JMA_PRECIP[idx] if idx < len(_JMA_PRECIP) else 0.0
                else:
                    precip_map[(i, j)] = 0.0
            else:
                precip_map[(i, j)] = 0.0
    ok = sum(1 for v in tile_cache.values() if v)
    print(f'[JMA] bt={bt} tiles={ok}/{len(tile_cache)} pts={len(precip_map)}')
    return precip_map

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
        '&current=cloud_cover,precipitation'
        '&hourly=cloud_cover,precipitation'
        '&forecast_days=2&timezone=UTC'
    )
    req = urllib.request.Request(url, headers={'User-Agent': 'rainbow-forecast/1.0'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    if isinstance(data, dict):
        data = [data]

    current_map = {}
    hourly_maps = {}
    hour_times  = None

    for (idx, _, _), item in zip(sample_points, data):
        c = item['current']
        current_map[idx] = {
            'cloud_cover':   c.get('cloud_cover', 0),
            'precipitation': c.get('precipitation', 0.0),
        }
        if hour_times is None:
            hour_times = item['hourly']['time']
        h_cc = item['hourly']['cloud_cover']
        h_pr = item['hourly']['precipitation']
        for h, (cc, pr) in enumerate(zip(h_cc, h_pr)):
            if h not in hourly_maps:
                hourly_maps[h] = {}
            hourly_maps[h][idx] = {
                'cloud_cover':   cc if cc is not None else 0,
                'precipitation': pr if pr is not None else 0.0,
            }

    return current_map, hourly_maps, hour_times or []

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

    current_map, hourly_maps, hour_times = fetch_weather_map()

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
        _build_and_upload_forecast(now, points, hourly_maps, hour_times)
        _refresh_sightings_json()
        print(f'[SKIP] {reason} sun_alt={sun_alt:.1f}°')
        return {'statusCode': 200, 'body': reason}

    anti_solar  = (sun_az + 180) % 360

    solar_cloud = 0
    if in_rainbow_window:
        sol_lat, sol_lon = offset_coordinate(CENTER_LAT, CENTER_LON, sun_az, 20)
        solar_cloud = lookup_weather(current_map, sol_lat, sol_lon)['cloud_cover']

    jma_map = None
    try:
        jma_map = fetch_jma_precip_map()
    except Exception as e:
        print(f'[JMA] unavailable: {e} — using Open-Meteo precipitation')

    features    = []
    for (lat, lon) in points:
        w = lookup_weather(current_map, lat, lon)
        local_precip = _get_precip_at(lat, lon, current_map, jma_map)
        if in_rainbow_window:
            rain  = has_directional_rain(current_map, lat, lon, anti_solar, jma_map=jma_map)
            score = rainbow_score(sun_alt, w['cloud_cover'], local_precip, rain, solar_cloud)
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
                'precip':           round(local_precip, 2),
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
    _build_and_upload_forecast(now, points, hourly_maps, hour_times)

    if in_rainbow_window:
        max_score = max((f['properties']['score'] for f in features), default=0)
        notify_nearby_devices(max_score, sun_az)
        print(f'[OK] {now.isoformat()} points={len(features)} max_score={max_score} sun_alt={sun_alt:.1f}°')
    else:
        print(f'[RAIN] {now.isoformat()} points={len(features)} sun_alt={sun_alt:.1f}° (rain display only)')

    _refresh_sightings_json()
    return {'statusCode': 200, 'body': f'Processed {len(features)} points'}

def _build_and_upload_forecast(now, points, hourly_maps, hour_times):
    """次の6時間分の虹スコア予報を forecast.json として S3 に保存する。"""
    frames = []
    for h_idx, time_str in enumerate(hour_times):
        try:
            frame_dt = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if frame_dt <= now:
            continue
        if len(frames) >= 6:
            break

        h_map = hourly_maps.get(h_idx)
        if h_map is None:
            continue

        h_alt, h_az = sun_position(frame_dt, CENTER_LAT, CENTER_LON)
        in_rainbow  = 0 <= h_alt <= 42
        anti_solar  = (h_az + 180) % 360

        h_solar_cloud = 0
        if in_rainbow:
            sol_lat, sol_lon = offset_coordinate(CENTER_LAT, CENTER_LON, h_az, 20)
            h_solar_cloud = lookup_weather(h_map, sol_lat, sol_lon)['cloud_cover']

        scores       = []
        precips      = []
        cloud_covers = []
        for (lat, lon) in points:
            w = lookup_weather(h_map, lat, lon)
            if in_rainbow:
                rain  = has_directional_rain(h_map, lat, lon, anti_solar)
                score = rainbow_score(h_alt, w['cloud_cover'], w['precipitation'], rain, h_solar_cloud)
            else:
                score = 0
            scores.append(score)
            precips.append(round(w['precipitation'], 2))
            cloud_covers.append(w['cloud_cover'])

        frames.append({
            'time_utc':    time_str,
            'sun_alt':     round(h_alt, 1),
            'sun_az':      round(h_az, 1),
            'in_rainbow':  in_rainbow,
            'scores':      scores,
            'precips':     precips,
            'cloud_covers': cloud_covers,
            'max_score':   max(scores),
        })

    forecast = {
        'generated_at': now.isoformat(),
        'frames':       frames,
    }
    boto3.client('s3').put_object(
        Bucket=BUCKET, Key='forecast.json',
        Body=json.dumps(forecast, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json', CacheControl='max-age=540',
    )
    print(f'[FORECAST] frames={len(frames)}')

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

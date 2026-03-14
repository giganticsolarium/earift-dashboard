import os
import json
import requests
from datetime import datetime, timedelta, timezone

# ── 환경변수에서 인증정보 읽기 ──────────────────────────────
ACCESS_TOKEN  = os.environ['META_ACCESS_TOKEN']
AD_ACCOUNT_ID = os.environ['META_AD_ACCOUNT_ID']   # act_1441821640906256
BASE_URL      = 'https://graph.facebook.com/v20.0'

KST = timezone(timedelta(hours=9))


# ── Meta API 호출 헬퍼 ──────────────────────────────────────
def api_get(path, params):
    params['access_token'] = ACCESS_TOKEN
    r = requests.get(f'{BASE_URL}/{path}', params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if 'error' in data:
        raise RuntimeError(f"Meta API error: {data['error']}")
    return data


def get_account_insights(date_preset):
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'spend,impressions,clicks,reach,ctr,cpc,cpm,actions,action_values',
        'date_preset': date_preset,
    })


def get_daily_insights():
    today = datetime.now(KST).date()
    since = today - timedelta(days=29)
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'date_start,spend,impressions,clicks,reach,ctr,cpc,actions,action_values',
        'time_increment': 1,
        'time_range': json.dumps({'since': str(since), 'until': str(today)}),
    })


def get_campaign_insights():
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'campaign_id,campaign_name,spend,impressions,clicks,reach,ctr,cpc,actions,action_values',
        'level': 'campaign',
        'date_preset': 'this_month',
        'limit': 25,
    })


def get_ad_insights():
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'ad_id,ad_name,adset_name,campaign_name,spend,impressions,clicks,actions,action_values',
        'level': 'ad',
        'date_preset': 'this_month',
        'sort': 'spend_descending',
        'limit': 10,
    })


# ── 데이터 파싱 헬퍼 ────────────────────────────────────────
def extract(items, action_type):
    """actions / action_values 배열에서 특정 타입 값 추출"""
    for item in (items or []):
        if item.get('action_type') == action_type:
            return float(item.get('value', 0))
    return 0.0


def parse_row(row):
    spend          = float(row.get('spend', 0))
    purchases      = extract(row.get('actions', []),       'purchase')
    purchase_value = extract(row.get('action_values', []), 'purchase')
    roas           = round(purchase_value / spend, 2) if spend > 0 else 0.0
    cpa            = round(spend / purchases, 0)     if purchases > 0 else 0.0
    return {
        'spend':          round(spend, 0),
        'impressions':    int(row.get('impressions', 0)),
        'clicks':         int(row.get('clicks', 0)),
        'reach':          int(row.get('reach', 0)),
        'ctr':            round(float(row.get('ctr', 0)), 2),
        'cpc':            round(float(row.get('cpc', 0)), 0),
        'cpm':            round(float(row.get('cpm', 0)), 0),
        'purchases':      int(purchases),
        'purchase_value': round(purchase_value, 0),
        'roas':           roas,
        'cpa':            cpa,
    }


def first_row(resp):
    rows = resp.get('data', [])
    return parse_row(rows[0]) if rows else {}


# ── 메인 수집 로직 ──────────────────────────────────────────
def main():
    now_kst = datetime.now(KST)
    print(f"[{now_kst.strftime('%Y-%m-%d %H:%M KST')}] Meta 광고 데이터 수집 시작")

    # ① 기간별 요약
    monthly   = first_row(get_account_insights('this_month'))
    today     = first_row(get_account_insights('today'))
    yesterday = first_row(get_account_insights('yesterday'))
    last_7d   = first_row(get_account_insights('last_7d'))

    summary = {
        'last_updated':     now_kst.isoformat(),
        'last_updated_kst': now_kst.strftime('%Y-%m-%d %H:%M KST'),
        'monthly':   monthly,
        'month':     monthly,   # HTML 호환용 alias
        'today':     today,
        'yesterday': yesterday,
        'last_7d':   last_7d,
    }

    # ② 일별 히스토리 (30일)
    daily_resp = get_daily_insights()
    daily_rows = []
    for row in daily_resp.get('data', []):
        parsed = parse_row(row)
        parsed['date'] = row.get('date_start', '')
        daily_rows.append(parsed)
    daily_rows.sort(key=lambda x: x['date'])

    # ③ 캠페인별
    camp_resp = get_campaign_insights()
    campaigns = []
    for row in camp_resp.get('data', []):
        parsed = parse_row(row)
        parsed['campaign_id']   = row.get('campaign_id', '')
        parsed['campaign_name'] = row.get('campaign_name', '')
        campaigns.append(parsed)
    campaigns.sort(key=lambda x: x['spend'], reverse=True)

    # ④ 광고(소재)별 TOP10
    ads_resp = get_ad_insights()
    ads = []
    for row in ads_resp.get('data', []):
        parsed = parse_row(row)
        parsed['ad_id']         = row.get('ad_id', '')
        parsed['ad_name']       = row.get('ad_name', '')
        parsed['adset_name']    = row.get('adset_name', '')
        parsed['campaign_name'] = row.get('campaign_name', '')
        ads.append(parsed)
    ads.sort(key=lambda x: x['spend'], reverse=True)

    # ── 파일 저장 ────────────────────────────────────────────
    os.makedirs('data', exist_ok=True)

    def save(filename, obj):
        path = f'data/{filename}'
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        print(f"  ✓ {path} 저장 완료")

    # summary에 캠페인·목표 포함 (HTML에서 s.campaigns, s.goals 사용)
    summary['campaigns'] = campaigns
    summary['goals'] = {
        'budget':  3000000,
        'roas':    2.5,
        'revenue': 7500000,
        'spend':   3000000,
    }

    save('summary.json',       summary)
    save('daily_history.json', {'last_updated': now_kst.isoformat(), 'data': daily_rows})
    save('campaigns.json',     {'last_updated': now_kst.isoformat(), 'campaigns': campaigns, 'data': campaigns})
    save('ads.json',           {'last_updated': now_kst.isoformat(), 'data': ads[:10]})

    print(f"\n✅ 수집 완료 | 이번달 지출: {monthly.get('spend',0):,.0f}원 | ROAS: {monthly.get('roas',0)}")


if __name__ == '__main__':
    main()

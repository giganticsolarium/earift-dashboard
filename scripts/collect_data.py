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
        'fields': 'date_start,spend,impressions,clicks,reach,ctr,cpc,cpm,actions,action_values',
        'time_increment': 1,
        'time_range': json.dumps({'since': str(since), 'until': str(today)}),
        'limit': 100,
    })


def get_campaign_insights():
    """이번달 캠페인 현황 — 현재 활성화된 캠페인만"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'campaign_id,campaign_name,spend,impressions,clicks,reach,ctr,cpc,cpm,actions,action_values',
        'level': 'campaign',
        'date_preset': 'this_month',
        'filtering': json.dumps([{"field": "campaign.effective_status", "operator": "IN", "value": ["ACTIVE"]}]),
        'limit': 25,
    })


def get_campaign_insights_today():
    """시간별 스냅샷용: 오늘 활성화된 캠페인별 실시간 데이터"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'campaign_id,campaign_name,spend,impressions,clicks,reach,ctr,cpc,cpm,actions,action_values',
        'level': 'campaign',
        'date_preset': 'today',
        'filtering': json.dumps([{"field": "campaign.effective_status", "operator": "IN", "value": ["ACTIVE"]}]),
        'limit': 25,
    })


def get_ad_insights_today():
    """시간별 스냅샷용: 오늘 활성화된 광고(소재)별 실시간 데이터"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'ad_id,ad_name,adset_name,campaign_name,spend,impressions,clicks,ctr,cpm,actions,action_values',
        'level': 'ad',
        'date_preset': 'today',
        'filtering': json.dumps([{"field": "ad.effective_status", "operator": "IN", "value": ["ACTIVE"]}]),
        'limit': 50,
    })


def get_campaign_insights_daily():
    """캠페인별 일별 성과 (이번달 전체)"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'date_start,campaign_id,campaign_name,spend,impressions,clicks,reach,ctr,cpc,cpm,actions,action_values',
        'level': 'campaign',
        'time_increment': 1,
        'date_preset': 'this_month',
        'limit': 200,
    })


def get_campaign_insights_for_period(date_preset):
    """기간별 캠페인 성과 (탭 기능용: last_month, this_week, last_week)"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'campaign_id,campaign_name,spend,impressions,clicks,reach,ctr,cpc,cpm,actions,action_values',
        'level': 'campaign',
        'date_preset': date_preset,
        'limit': 25,
    })


def get_ad_insights():
    """금일 콘텐츠 TOP — 오늘 활성화된 광고만"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'ad_id,ad_name,adset_name,campaign_name,spend,impressions,clicks,ctr,cpm,actions,action_values',
        'level': 'ad',
        'date_preset': 'today',
        'filtering': json.dumps([{"field": "ad.effective_status", "operator": "IN", "value": ["ACTIVE"]}]),
        'sort': 'spend_descending',
        'limit': 20,
    })


def get_ad_insights_yesterday():
    """어제 광고(소재)별 성과 — 전날 대비 비교용"""
    return api_get(AD_ACCOUNT_ID + '/insights', {
        'fields': 'ad_id,ad_name,adset_name,campaign_name,spend,impressions,clicks,ctr,cpm,actions,action_values',
        'level': 'ad',
        'date_preset': 'yesterday',
        'sort': 'spend_descending',
        'limit': 20,
    })


def get_activity_log(since_ts, until_ts):
    """광고 운영 변경 이력 — on/off, 예산 수정 등
    since_ts / until_ts : Unix timestamp (int)
    Meta /activities 엔드포인트는 date string 이 아닌 Unix timestamp 를 요구함
    """
    return api_get(AD_ACCOUNT_ID + '/activities', {
        'fields': 'actor_name,event_type,event_time,extra_data,object_id,object_type',
        'since': since_ts,
        'until': until_ts,
        'limit': 200,
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

    # Meta API 일별 데이터는 3~5일 지연이 있으므로
    # today / yesterday 실시간 데이터를 daily_rows에 직접 보완
    today_str     = now_kst.strftime('%Y-%m-%d')
    yesterday_str = (now_kst - timedelta(days=1)).strftime('%Y-%m-%d')
    existing_dates = {r['date'] for r in daily_rows}

    for date_str, preset in [(yesterday_str, 'yesterday'), (today_str, 'today')]:
        if date_str not in existing_dates and summary.get(preset):
            row = dict(summary[preset])
            row['date'] = date_str
            daily_rows.append(row)

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

    # ④ 광고(소재)별 TOP20 — 오늘
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

    # ④-b 어제 광고(소재)별 — 전날 대비 비교용
    try:
        yesterday_ads_resp = get_ad_insights_yesterday()
        yesterday_ads = []
        for row in yesterday_ads_resp.get('data', []):
            parsed = parse_row(row)
            parsed['ad_id']         = row.get('ad_id', '')
            parsed['ad_name']       = row.get('ad_name', '')
            parsed['adset_name']    = row.get('adset_name', '')
            parsed['campaign_name'] = row.get('campaign_name', '')
            yesterday_ads.append(parsed)
        yesterday_ads.sort(key=lambda x: x['spend'], reverse=True)
        print(f"  ✓ yesterday_ads 수집 완료: {len(yesterday_ads)}개")
    except Exception as e:
        print(f"  ⚠️ yesterday_ads 수집 실패 (비교 기능 미작동, 나머지 영향 없음): {e}")
        yesterday_ads = []

    # ⑤ 오늘 캠페인·광고 실시간 (시간별 스냅샷용) — 활성화된 것만
    today_camp_resp = get_campaign_insights_today()
    today_campaigns = []
    for row in today_camp_resp.get('data', []):
        parsed = parse_row(row)
        parsed['campaign_id']   = row.get('campaign_id', '')
        parsed['campaign_name'] = row.get('campaign_name', '')
        today_campaigns.append(parsed)
    today_campaigns.sort(key=lambda x: x['spend'], reverse=True)

    try:
        today_ads_resp = get_ad_insights_today()
        today_ads_snap = []
        for row in today_ads_resp.get('data', []):
            parsed = parse_row(row)
            parsed['ad_id']         = row.get('ad_id', '')
            parsed['ad_name']       = row.get('ad_name', '')
            parsed['adset_name']    = row.get('adset_name', '')
            parsed['campaign_name'] = row.get('campaign_name', '')
            today_ads_snap.append(parsed)
        today_ads_snap.sort(key=lambda x: x['spend'], reverse=True)
        print(f"  ✓ today_ads 수집 완료: {len(today_ads_snap)}개")
    except Exception as e:
        print(f"  ⚠️ today_ads 수집 실패 (스냅샷에 ads 미포함, 나머지 수집 계속): {e}")
        today_ads_snap = []

    # ── 파일 저장 ────────────────────────────────────────────
    os.makedirs('data', exist_ok=True)

    def save(filename, obj):
        path = f'data/{filename}'
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        print(f"  ✓ {path} 저장 완료")

    def load_json(filename, default):
        path = f'data/{filename}'
        if os.path.exists(path):
            try:
                with open(path, encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return default

    # summary에 캠페인·목표 포함 (HTML에서 s.campaigns, s.goals 사용)
    summary['campaigns'] = campaigns
    # goals.json에서 목표 값 읽기 (없으면 기본값 사용)
    _default_goals = {'budget': 3000000, 'roas': 2.5, 'revenue': 7500000, 'spend': 3000000}
    summary['goals'] = load_json('goals.json', _default_goals)

    # ── 일별 히스토리 장기 누적 (30일 이상 보관) ──────────────
    cutoff_date = (now_kst - timedelta(days=5)).strftime('%Y-%m-%d')
    old_daily   = load_json('daily_history.json', {}).get('data', [])
    merged_daily = {r['date']: r for r in old_daily if r.get('date', '') < cutoff_date}
    for r in daily_rows:                          # 최근 5일은 항상 최신으로 덮어씌움
        merged_daily[r['date']] = r
    merged_daily_list = sorted(merged_daily.values(), key=lambda x: x['date'])

    # ── 시간별 스냅샷 누적 ────────────────────────────────────
    snapshot_key = f"{today_str}T{now_kst.hour:02d}"
    old_snaps    = load_json('hourly_snapshots.json', {}).get('snapshots', [])
    snap_dict    = {s['key']: s for s in old_snaps}  # 중복 방지
    snap_dict[snapshot_key] = {
        'key':      snapshot_key,
        'datetime': now_kst.isoformat(),
        'date':     today_str,
        'hour':     now_kst.hour,
        'totals':    today,                  # 오늘 전체 실시간
        'campaigns': today_campaigns,       # 오늘 캠페인별 실시간 (활성화만)
        'ads':       today_ads_snap,        # 오늘 광고(소재)별 실시간 (활성화만)
    }
    snaps_list = sorted(snap_dict.values(), key=lambda x: x['key'])

    # ⑥ 기간별 캠페인 데이터 (period_campaigns.json) - 탭 기능용
    # Meta API 유효 preset: this_week_mon_today / last_week_mon_sun
    period_camps = {
        'this_month': campaigns,      # 이미 수집된 이번달 데이터 재활용
        'today':      today_campaigns, # 이미 수집된 오늘 실시간 데이터 재활용
    }
    period_api_map = {
        'last_month': 'last_month',
        'this_week':  'this_week_mon_today',
        'last_week':  'last_week_mon_sun',
    }
    for key, api_preset in period_api_map.items():
        try:
            resp = get_campaign_insights_for_period(api_preset)
            plist = []
            for row in resp.get('data', []):
                parsed = parse_row(row)
                parsed['campaign_id']   = row.get('campaign_id', '')
                parsed['campaign_name'] = row.get('campaign_name', '')
                plist.append(parsed)
            plist.sort(key=lambda x: x['spend'], reverse=True)
            period_camps[key] = plist
        except Exception as e:
            print(f"  ⚠️ {key} 캠페인 수집 실패 (대시보드 영향 없음): {e}")
            period_camps[key] = []

    # ⑦ 캠페인별 일별 성과 (보고서용) - 실패해도 기존 수집에 영향 없음
    camp_daily_rows = []
    try:
        camp_daily_resp = get_campaign_insights_daily()
        for row in camp_daily_resp.get('data', []):
            parsed = parse_row(row)
            parsed['campaign_id']   = row.get('campaign_id', '')
            parsed['campaign_name'] = row.get('campaign_name', '')
            parsed['date']          = row.get('date_start', '')
            camp_daily_rows.append(parsed)
        camp_daily_rows.sort(key=lambda x: (x['date'], x['campaign_name']))
    except Exception as e:
        print(f"  ⚠️ campaign_daily 수집 실패 (대시보드 영향 없음): {e}")

    # 기존 파일 먼저 저장 (대시보드 핵심 데이터)
    save('summary.json',          summary)
    save('daily_history.json',    {'last_updated': now_kst.isoformat(), 'data': merged_daily_list})
    save('campaigns.json',        {'last_updated': now_kst.isoformat(), 'campaigns': campaigns, 'data': campaigns})
    save('ads.json',              {'last_updated': now_kst.isoformat(), 'data': ads[:20], 'yesterday_data': yesterday_ads[:20]})
    save('hourly_snapshots.json', {'last_updated': now_kst.isoformat(), 'snapshots': snaps_list})
    save('period_campaigns.json', {'last_updated': now_kst.isoformat(), **period_camps})

    # campaign_daily는 별도 try-except (실패해도 기존 데이터에 영향 없음)
    try:
        save('campaign_daily.json', {'last_updated': now_kst.isoformat(), 'data': camp_daily_rows})
    except Exception as e:
        print(f"  ⚠️ campaign_daily.json 저장 실패 (대시보드 영향 없음): {e}")

    # ⑧ 광고 운영 변경 이력 (activity_log.json) — 증분 수집 후 누적 저장
    import calendar, traceback as _tb
    print("\n  [activity_log] 수집 시작...")

    def date_to_unix(date_str):
        """YYYY-MM-DD → UTC 기준 Unix timestamp (int)"""
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return int(calendar.timegm(dt.timetuple()))

    try:
        # 기존 누적 데이터 불러오기
        old_act    = load_json('activity_log.json', {})
        old_events = old_act.get('events', [])

        # 수집 기간 결정: 기존 데이터 있으면 최근 2일만 재수집, 없으면 최초 30일
        if old_events:
            all_times   = [e['event_time'][:10] for e in old_events if e.get('event_time')]
            latest_date = max(all_times) if all_times else today_str
            fetch_since = (datetime.strptime(latest_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            fetch_since = (now_kst - timedelta(days=30)).strftime('%Y-%m-%d')
        fetch_until = today_str

        since_ts = date_to_unix(fetch_since)
        until_ts = date_to_unix(fetch_until) + 86399  # 해당 날짜 23:59:59 까지 포함
        print(f"  [activity_log] 조회 기간: {fetch_since} ~ {fetch_until}  (ts: {since_ts}~{until_ts})")

        act_resp   = get_activity_log(since_ts, until_ts)
        raw_count  = len(act_resp.get('data', []))
        print(f"  [activity_log] API 응답: {raw_count}건")

        new_events = []
        for item in act_resp.get('data', []):
            extra = {}
            try:
                extra = json.loads(item.get('extra_data', '{}') or '{}')
            except Exception:
                pass

            event_type_raw = item.get('event_type', '')
            if 'status' in event_type_raw.lower() or event_type_raw in (
                    'ad_enabled', 'ad_disabled',
                    'adset_enabled', 'adset_disabled',
                    'campaign_enabled', 'campaign_disabled'):
                new_status = extra.get('new_value', extra.get('status', ''))
                category = 'status_on' if str(new_status).upper() in ('ACTIVE', '1', 'TRUE', 'ON') else 'status_off'
            elif 'budget' in event_type_raw.lower() or 'bid' in event_type_raw.lower():
                def _num(v):
                    # Meta API가 예산을 {"value": 50000, "currency": "KRW"} 형태로 반환할 수 있음
                    if isinstance(v, dict):
                        v = v.get('value', v.get('amount', 0))
                    try:
                        return float(v or 0)
                    except (TypeError, ValueError):
                        return 0.0
                old_b = _num(extra.get('old_value', extra.get('old_budget', 0)))
                new_b = _num(extra.get('new_value', extra.get('new_budget', 0)))
                category = 'budget_up' if new_b >= old_b else 'budget_down'
            else:
                category = 'edit'

            new_events.append({
                'event_time':  item.get('event_time', ''),
                'event_type':  event_type_raw,
                'category':    category,
                'object_id':   str(item.get('object_id', '')),
                'object_name': item.get('object_name', item.get('object_id', '')),  # object_name 없으면 id로 대체
                'object_type': item.get('object_type', ''),
                'actor_name':  item.get('actor_name', ''),
                'extra':       extra,
            })

        # 기존 이벤트와 병합 — event_time + object_id + event_type 기준 중복 제거
        def evt_key(e):
            return f"{e['event_time']}_{e['object_id']}_{e['event_type']}"

        merged = {evt_key(e): e for e in old_events}
        for e in new_events:
            merged[evt_key(e)] = e

        # 최신순 정렬 후 90일 초과분 제거
        cutoff_act  = (now_kst - timedelta(days=90)).strftime('%Y-%m-%d')
        merged_list = sorted(merged.values(), key=lambda x: x['event_time'], reverse=True)
        merged_list = [e for e in merged_list if e['event_time'][:10] >= cutoff_act]

        save('activity_log.json', {
            'last_updated': now_kst.isoformat(),
            'events':       merged_list,
        })
        print(f"  ✓ activity_log.json 저장 완료: 신규 {len(new_events)}건 / 누적 {len(merged_list)}건")

    except Exception as e:
        print(f"  ⚠️ activity_log.json 수집/저장 실패: {e}")
        print(f"  ⚠️ 상세 오류:\n{_tb.format_exc()}")

    print(f"\n✅ 수집 완료 | 이번달 지출: {monthly.get('spend',0):,.0f}원 | ROAS: {monthly.get('roas',0)}")


if __name__ == '__main__':
    main()

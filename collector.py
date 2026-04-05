import os, requests, json, datetime, pytz
from pathlib import Path
from collections import Counter, defaultdict

API_KEYS = [
    os.environ["AVIATIONSTACK_KEY_1"],
    os.environ["AVIATIONSTACK_KEY_2"],
    os.environ["AVIATIONSTACK_KEY_3"],
    os.environ["AVIATIONSTACK_KEY_4"],
]
TG_TOKEN      = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TG_CHAT_ID", "")
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")

BASE = "https://api.aviationstack.com/v1/flights"
DOMESTIC = {'BKK','DMK','CNX','USM','HDY','UTP','KBV','TST','NAW','THS','UBP','CEI','NST','URT','HHQ','CJM'}

COUNTRY_FLAGS = {
    'Russia': '🇷🇺', 'China': '🇨🇳', 'India': '🇮🇳', 'UAE': '🇦🇪', 'Singapore': '🇸🇬',
    'Malaysia': '🇲🇾', 'Indonesia': '🇮🇩', 'Thailand': '🇹🇭', 'Vietnam': '🇻🇳', 'South Korea': '🇰🇷',
    'Japan': '🇯🇵', 'Australia': '🇦🇺', 'New Zealand': '🇳🇿', 'UK': '🇬🇧', 'Germany': '🇩🇪',
    'France': '🇫🇷', 'Italy': '🇮🇹', 'Spain': '🇪🇸', 'Netherlands': '🇳🇱', 'Turkey': '🇹🇷',
    'Saudi Arabia': '🇸🇦', 'Qatar': '🇶🇦', 'Kuwait': '🇰🇼', 'Oman': '🇴🇲', 'Bahrain': '🇧🇭',
    'Jordan': '🇯🇴', 'Israel': '🇮🇱', 'Kazakhstan': '🇰🇿', 'Uzbekistan': '🇺🇿', 'Azerbaijan': '🇦🇿',
    'Armenia': '🇦🇲', 'Georgia': '🇬🇪', 'Kyrgyzstan': '🇰🇬', 'Tajikistan': '🇹🇯', 'Turkmenistan': '🇹🇲',
    'Pakistan': '🇵🇰', 'Bangladesh': '🇧🇩', 'Sri Lanka': '🇱🇰', 'Nepal': '🇳🇵', 'Cambodia': '🇰🇭',
    'Myanmar': '🇲🇲', 'Laos': '🇱🇦', 'Brunei': '🇧🇳', 'Hong Kong': '🇭🇰', 'Taiwan': '🇹🇼',
    'Macau': '🇲🇴', 'Maldives': '🇲🇻', 'Greece': '🇬🇷', 'Poland': '🇵🇱', 'Czechia': '🇨🇿',
    'Austria': '🇦🇹', 'Hungary': '🇭🇺', 'Latvia': '🇱🇻', 'Estonia': '🇪🇪', 'Finland': '🇫🇮',
    'Sweden': '🇸🇪', 'Norway': '🇳🇴', 'Denmark': '🇩🇰', 'Switzerland': '🇨🇭', 'Portugal': '🇵🇹',
    'Ukraine': '🇺🇦', 'Belarus': '🇧🇾', 'Lithuania': '🇱🇹', 'Moldova': '🇲🇩', 'Bulgaria': '🇧🇬',
    'Romania': '🇷🇴', 'Serbia': '🇷🇸', 'Croatia': '🇭🇷', 'Slovenia': '🇸🇮', 'Slovakia': '🇸🇰',
    'Luxembourg': '🇱🇺', 'Ireland': '🇮🇪', 'Iceland': '🇮🇸', 'Cyprus': '🇨🇾', 'Malta': '🇲🇹',
}

MAP_C = {
    'SVO':'Russia','DME':'Russia','VKO':'Russia','LED':'Russia','KZN':'Russia','SVX':'Russia','OVB':'Russia',
    'KJA':'Russia','AER':'Russia','KRR':'Russia','ROV':'Russia','UFA':'Russia','MRV':'Russia','STW':'Russia',
    'ASF':'Russia','EIK':'Russia','KGD':'Russia','GOJ':'Russia','KUF':'Russia','NAL':'Russia','MQF':'Russia',
    'TJM':'Russia','NJC':'Russia','SGC':'Russia','HTA':'Russia','BQS':'Russia','DYR':'Russia','IKT':'Russia',
    'VVO':'Russia','KHV':'Russia','PKC':'Russia','UUS':'Russia','GDX':'Russia','YKS':'Russia','BTK':'Russia',
    'PEZ':'Russia','SCW':'Russia','USY':'Russia','VKT':'Russia','IJK':'Russia','OGZ':'Russia','MCX':'Russia',
    'GRV':'Russia','ESL':'Russia','VKZ':'Russia','KLF':'Russia','BZK':'Russia','KGP':'Russia',
    'ALA':'Kazakhstan','NQZ':'Kazakhstan','KGF':'Kazakhstan','GUW':'Kazakhstan','SCO':'Kazakhstan',
    'PLX':'Kazakhstan','PWQ':'Kazakhstan','URA':'Kazakhstan','AKX':'Kazakhstan','KSN':'Kazakhstan',
    'CIT':'Kazakhstan','DMB':'Kazakhstan','UKK':'Kazakhstan','ZHA':'Kazakhstan',
    'PEK':'China','PVG':'China','CAN':'China','SZX':'China','CTU':'China','HAK':'China','XIY':'China',
    'CKG':'China','WUH':'China','NKG':'China','TAO':'China','XMN':'China','CGO':'China','CSX':'China',
    'DLC':'China','TSN':'China','KMG':'China','NNG':'China','KWL':'China','FOC':'China','HGH':'China',
    'WNZ':'China','JJN':'China','LYA':'China','SWA':'China','TNA':'China','YNT':'China','ZUH':'China',
    'NGB':'China','CGQ':'China','HRB':'China','SJW':'China','TYN':'China','URC':'China','LHW':'China',
    'HFE':'China','KWE':'China','CGD':'China','YIH':'China','XFN':'China','MXZ':'China',
    'JHG':'China','LJG':'China','DLU':'China','XSY':'China','WUX':'China',
    'DEL':'India','BOM':'India','BLR':'India','MAA':'India','HYD':'India','CCU':'India','GOI':'India',
    'AMD':'India','PNQ':'India','JAI':'India','LKO':'India','GAU':'India','TRV':'India','IXC':'India',
    'COK':'India','STV':'India','VNS':'India','IXB':'India','IXR':'India','IXM':'India','VTZ':'India',
    'IXZ':'India','JRH':'India','DMU':'India','IXW':'India','RPR':'India','BBI':'India','BHO':'India',
    'UDR':'India','IDR':'India','NAG':'India','RAJ':'India','BDQ':'India','IXJ':'India','SXR':'India',
    'DXB':'UAE','AUH':'UAE','SHJ':'UAE','DOH':'Qatar','KWI':'Kuwait','RUH':'Saudi Arabia','JED':'Saudi Arabia',
    'DMM':'Saudi Arabia','BAH':'Bahrain','MCT':'Oman','AMM':'Jordan','TLV':'Israel','BEY':'Lebanon',
    'LHR':'UK','MAN':'UK','LGW':'UK','STN':'UK','FRA':'Germany','MUC':'Germany','DUS':'Germany','BER':'Germany',
    'CDG':'France','ORY':'France','FCO':'Italy','MXP':'Italy','VCE':'Italy','AMS':'Netherlands','MAD':'Spain',
    'BCN':'Spain','VIE':'Austria','PRG':'Czechia','WAW':'Poland','BUD':'Hungary','RIX':'Latvia','TLL':'Estonia',
    'HEL':'Finland','ARN':'Sweden','CPH':'Denmark','OSL':'Norway','ZRH':'Switzerland','GVA':'Switzerland',
    'LIS':'Portugal','ATH':'Greece','SIN':'Singapore','KUL':'Malaysia','PEN':'Malaysia','HKG':'Hong Kong',
    'TPE':'Taiwan','ICN':'South Korea','GMP':'South Korea','NRT':'Japan','HND':'Japan','KIX':'Japan',
    'SYD':'Australia','MEL':'Australia','BNE':'Australia','PER':'Australia','AKL':'New Zealand','CHC':'New Zealand',
    'TAS':'Uzbekistan','SKD':'Uzbekistan','NVI':'Uzbekistan','UGC':'Uzbekistan',
    'GYD':'Azerbaijan','NAJ':'Azerbaijan',
    'EVN':'Armenia',
    'FRU':'Kyrgyzstan','OSS':'Kyrgyzstan',
    'DYU':'Tajikistan','LBD':'Tajikistan','KQT':'Tajikistan',
    'ASB':'Turkmenistan','CRZ':'Turkmenistan','MYP':'Turkmenistan',
    'TBS':'Georgia','BUS':'Georgia','KUT':'Georgia',
    'KIV':'Moldova',
    'MLE':'Maldives','SGN':'Vietnam','HAN':'Vietnam','PNH':'Cambodia','RGN':'Myanmar','DPS':'Indonesia',
    'JOG':'Indonesia','SUB':'Indonesia','UPG':'Indonesia','REP':'Cambodia','DAC':'Bangladesh','KHI':'Pakistan',
    'CMB':'Sri Lanka','KTM':'Nepal','ISB':'Pakistan','LHE':'Pakistan','PEW':'Pakistan','MUX':'Pakistan',
    'CJU':'South Korea','PUS':'South Korea','FUK':'Japan','OKA':'Japan','NGO':'Japan','BKK':'Thailand',
    'DMK':'Thailand','CNX':'Thailand','USM':'Thailand','HDY':'Thailand','KBV':'Thailand','TST':'Thailand',
    'NAW':'Thailand','THS':'Thailand','UBP':'Thailand','CEI':'Thailand','NST':'Thailand','URT':'Thailand',
    'HHQ':'Thailand','CJM':'Thailand','KCH':'Malaysia','BKI':'Malaysia','TGG':'Malaysia','SDK':'Malaysia',
    'BWN':'Brunei','MFM':'Macau','VTE':'Laos','LPQ':'Laos','MDL':'Myanmar','NYU':'Myanmar','HEH':'Myanmar',
    'VCA':'Vietnam','PQC':'Vietnam','CXR':'Vietnam','DAD':'Vietnam','UIH':'Vietnam','VKG':'Vietnam','DLI':'Vietnam',
    'KOS':'Cambodia','HRI':'Sri Lanka','RML':'Sri Lanka','TRR':'Sri Lanka','PKR':'Nepal','BIR':'Nepal',
    'TKG':'Indonesia','PDG':'Indonesia','PKU':'Indonesia','BTH':'Indonesia','TNJ':'Indonesia','PLM':'Indonesia',
    'KBP':'Ukraine','ODS':'Ukraine','HRK':'Ukraine','DNK':'Ukraine','IEV':'Ukraine','LWO':'Ukraine',
    'MSQ':'Belarus','GME':'Belarus','VNO':'Lithuania','KUN':'Lithuania','LPX':'Latvia','URE':'Estonia',
    'TMP':'Finland','TKU':'Finland','OUL':'Finland','KUO':'Finland','RVN':'Finland','GOT':'Sweden','MMX':'Sweden',
    'BLL':'Denmark','AAL':'Denmark','RKE':'Denmark','BGO':'Norway','TRD':'Norway','SVG':'Norway','TOS':'Norway',
    'BSL':'Switzerland','BRN':'Switzerland','LUG':'Switzerland','OPO':'Portugal','FAO':'Portugal','FNC':'Portugal',
    'SKG':'Greece','HER':'Greece','RHO':'Greece','CFU':'Greece','ZTH':'Greece','IST':'Turkey','SAW':'Turkey',
    'ADB':'Turkey','AYT':'Turkey','ESB':'Turkey','DLM':'Turkey','BJV':'Turkey','TZX':'Turkey','KYA':'Turkey'
}

# Russian airport → city name (Russian)
MAP_RU_CITY = {
    'SVO': 'Москва', 'DME': 'Москва', 'VKO': 'Москва',
    'LED': 'Санкт-Петербург',
    'KZN': 'Казань',
    'SVX': 'Екатеринбург',
    'OVB': 'Новосибирск',
    'KJA': 'Красноярск',
    'AER': 'Сочи',
    'KRR': 'Краснодар',
    'ROV': 'Ростов-на-Дону',
    'UFA': 'Уфа',
    'MRV': 'Минеральные Воды',
    'STW': 'Ставрополь',
    'ASF': 'Астрахань',
    'EIK': 'Ейск',
    'KGD': 'Калининград',
    'GOJ': 'Нижний Новгород',
    'KUF': 'Самара',
    'NAL': 'Нальчик',
    'MQF': 'Магнитогорск',
    'TJM': 'Тюмень',
    'NJC': 'Нижневартовск',
    'SGC': 'Сургут',
    'HTA': 'Чита',
    'BQS': 'Благовещенск',
    'DYR': 'Анадырь',
    'IKT': 'Иркутск',
    'VVO': 'Владивосток',
    'KHV': 'Хабаровск',
    'PKC': 'Петропавловск-Камчатский',
    'UUS': 'Южно-Сахалинск',
    'GDX': 'Магадан',
    'YKS': 'Якутск',
    'BTK': 'Братск',
    'PEZ': 'Пенза',
    'SCW': 'Сыктывкар',
    'USY': 'Усинск',
    'VKT': 'Воркута',
    'IJK': 'Ижевск',
    'OGZ': 'Владикавказ',
    'MCX': 'Махачкала',
    'GRV': 'Грозный',
    'ESL': 'Элиста',
    'VKZ': 'Великий Новгород',
    'KLF': 'Калуга',
    'BZK': 'Брянск',
    'KGP': 'Когалым',
}

CAP = {'319':140,'320':180,'321':220,'332':280,'333':320,'339':350,'359':325,'388':525,'737':180,'738':189,'739':220,'73J':220,'772':314,'773':365,'77W':396,'77L':314,'788':250,'789':290,'78J':330,'32A':180,'32B':186,'32N':186,'32Q':186,'E90':100,'E95':120,'CR9':90,'DH4':78,'AT7':70,'CRJ':90,'32S':180}
LOAD_FACTOR = 0.82  # средняя загрузка международных рейсов в Таиланд

# ── Russian transit estimation ────────────────────────────────────────────────
HKT_RUSSIAN_SHARE = 0.38   # Пхукет получает ~38% россиян, въезжающих в Таиланд
                            # Источник: TAT/MOTS годовые отчёты 2023-2024. Проверять ежегодно.

TRANSIT_HUBS = ['Turkey', 'China', 'India']  # хабы с транзитным российским трафиком
                                              # UAE и Qatar исключены: россияне этот маршрут не используют (2026)


def load_tat_stats():
    """Загружает data/tat_stats.json. Возвращает {} при ошибке."""
    p = Path("data/tat_stats.json")
    if not p.exists():
        print("Warning: data/tat_stats.json not found — russia transit will be 0")
        return {}
    try:
        return json.loads(p.read_text()).get("monthly", {})
    except Exception as e:
        print(f"Warning: Failed to read tat_stats.json: {e}")
        return {}


def tat_monthly_avg(tat_monthly, ref_date, lookback=3):
    """
    Возвращает среднемесячное количество российских туристов (весь Таиланд)
    за `lookback` месяцев до ref_date. Fallback: 130 000/мес.
    """
    counts = []
    for i in range(1, lookback + 4):   # буфер для пропущенных месяцев
        m = ref_date.replace(day=1) - datetime.timedelta(days=28 * i)
        key = f"{m.year}-{m.month:02d}"
        if key in tat_monthly:
            counts.append(tat_monthly[key])
        if len(counts) == lookback:
            break
    if not counts:
        return 130000.0   # глобальный fallback: среднемесячное за 2024
    return sum(counts) / len(counts)


def calc_russian_transit(period_days, countries_arrivals, tat_monthly, ref_date):
    """
    Оценивает количество российских пассажиров, прибывших через транзитные хабы.

    Формула:
        hkt_russians = tat_avg_monthly * HKT_RUSSIAN_SHARE / 30.44 * period_days
        transit = max(0, hkt_russians - direct_russia_pax)
        распределяем transit пропорционально трафику хабов

    Возвращает: {"flights": int, "pax": int, "estimated": True}
    """
    monthly_avg = tat_monthly_avg(tat_monthly, ref_date)
    daily_hkt = (monthly_avg * HKT_RUSSIAN_SHARE) / 30.44
    period_hkt_russians = daily_hkt * period_days

    direct_pax = sum(
        v.get("pax", 0)
        for c, v in countries_arrivals.items()
        if v.get("country") == "Russia" or c == "Russia"
    )

    transit_total = max(0.0, period_hkt_russians - direct_pax)
    if transit_total < 1:
        return {"flights": 0, "pax": 0, "estimated": True}

    hub_pax = {
        hub: sum(
            v.get("pax", 0)
            for c, v in countries_arrivals.items()
            if v.get("country", c) == hub or c == hub
        )
        for hub in TRANSIT_HUBS
    }
    total_hub_pax = sum(hub_pax.values())

    transit_pax = int(transit_total)
    avg_flight_pax = 180 * LOAD_FACTOR

    if total_hub_pax == 0:
        transit_flights = max(1, round(transit_pax / avg_flight_pax))
        return {"flights": transit_flights, "pax": transit_pax, "estimated": True}

    transit_flights = max(1, round(transit_pax / avg_flight_pax))
    return {"flights": transit_flights, "pax": transit_pax, "estimated": True}

ICT = pytz.timezone('Asia/Bangkok')

MONTH_NAMES_RU = {1:'Янв',2:'Фев',3:'Мар',4:'Апр',5:'Май',6:'Июн',7:'Июл',8:'Авг',9:'Сен',10:'Окт',11:'Ноя',12:'Дек'}
DAY_NAMES_RU   = {0:'Пн',1:'Вт',2:'Ср',3:'Чт',4:'Пт',5:'Сб',6:'Вс'}

def get_api_key():
    # 6 runs/day across 4 keys = 12 API calls/day = 360/month (лимит 400)
    # 00:30 ICT → KEY_1
    # 05:00 ICT → KEY_2
    # 09:00 ICT → KEY_3
    # 13:00 ICT → KEY_4
    # 17:00 ICT → KEY_1
    # 23:58 ICT → KEY_2  (day finalisation)
    now = datetime.datetime.now(ICT)
    h = now.hour
    if h < 3:    return API_KEYS[0], 1   # 00:30 ICT
    elif h < 7:  return API_KEYS[1], 2   # 05:00 ICT
    elif h < 11: return API_KEYS[2], 3   # 09:00 ICT
    elif h < 15: return API_KEYS[3], 4   # 13:00 ICT
    elif h < 22: return API_KEYS[0], 1   # 17:00 ICT
    else:        return API_KEYS[1], 2   # 23:58 ICT

def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception: pass

def fetch_flights(direction, api_key):
    """Fetch with automatic fallback to other keys if primary is exhausted."""
    key_param = "arr_iata" if direction == "arrival" else "dep_iata"
    # Build priority list: primary key first, then others
    keys_to_try = [api_key] + [k for k in API_KEYS if k != api_key]
    for i, k in enumerate(keys_to_try):
        try:
            r = requests.get(BASE, params={"access_key": k, key_param: "HKT", "limit": 100}, timeout=20)
            data = r.json()
            # AviationStack returns error object when limit exceeded
            if "error" in data:
                code = data["error"].get("code", "")
                print(f"⚠️ Key #{i+1} error: {code} — trying next key")
                continue
            r.raise_for_status()
            if i > 0:
                print(f"ℹ️ Used fallback key #{i+1}")
            return data.get("data", []), i + 1
        except Exception as e:
            print(f"⚠️ Key #{i+1} exception: {e} — trying next key")
            continue
    print("❌ All API keys failed")
    return [], 0

def analyze(flights, direction):
    by_date = {}
    for f in flights:
        d = f.get("flight_date")
        if d: by_date.setdefault(d, []).append(f)
    res = {}
    for date, fl in by_date.items():
        cnt, pax, st = 0, 0, {"completed":0,"upcoming":0,"cancelled":0}
        ctry = defaultdict(lambda: {"flights":0, "pax":0})
        flight_list = []   # individual arrival records for display
        for f in fl:
            dep = (f.get("departure") or {}).get("iata", "")
            arr = (f.get("arrival") or {}).get("iata", "")
            if (direction=="arrival" and dep in DOMESTIC) or (direction=="departure" and arr in DOMESTIC): continue
            cnt += 1
            ac_iata = (f.get("aircraft") or {}).get("iata", "")
            flight_pax = int(CAP.get(ac_iata, 180) * LOAD_FACTOR)
            pax += flight_pax
            airport = dep if direction=="arrival" else arr
            country = MAP_C.get(airport, f"Other({airport})")
            if country == "Russia":
                city = MAP_RU_CITY.get(airport, airport)
                ctry[city]["flights"] += 1
                ctry[city]["pax"] += flight_pax
                ctry[city]["country"] = "Russia"
            else:
                ctry[country]["flights"] += 1
                ctry[country]["pax"] += flight_pax
            s = (f.get("flight_status") or "").lower().strip()
            if direction == "departure":
                if s in ("departed","landed","diverted","active","en route","en-route","incidents","taxi-out","pushback","returned","taxi"): st["completed"]+=1
                elif s in ("scheduled","taxiing","boarding","expected","estimated","gate","holding"): st["upcoming"]+=1
                elif s == "cancelled": st["cancelled"]+=1
            else:
                if s in ("landed","arrived","diverted"): st["completed"]+=1
                elif s in ("scheduled","active","en route","en-route","taxiing","taxi","boarding","expected","estimated"): st["upcoming"]+=1
                elif s == "cancelled": st["cancelled"]+=1
            # Collect individual flight records for board display
            fn = (f.get("flight") or {}).get("iata") or (f.get("flight") or {}).get("icao", "")
            airline = (f.get("airline") or {}).get("name", "")
            dep_sched = (f.get("departure") or {}).get("scheduled", "")
            arr_sched = (f.get("arrival")   or {}).get("scheduled", "")
            if direction == "arrival":
                flight_list.append({
                    "fn":       fn,
                    "airline":  airline,
                    "from":     dep,
                    "country":  MAP_C.get(dep, ""),
                    "status":   s,
                    "pax":      flight_pax,
                    "dep_time": dep_sched[:16] if dep_sched else "",
                    "arr_time": arr_sched[:16] if arr_sched else "",
                })
            else:
                flight_list.append({
                    "fn":       fn,
                    "airline":  airline,
                    "to":       arr,
                    "country":  MAP_C.get(arr, ""),
                    "status":   s,
                    "pax":      flight_pax,
                    "dep_time": dep_sched[:16] if dep_sched else "",
                    "arr_time": arr_sched[:16] if arr_sched else "",
                })
        res[date] = {"count": cnt, "pax": pax, "countries": dict(ctry), "stats": st,
                     "flight_list": flight_list}
    return res

def load_period_stats(today_str, days):
    tot_a = {"count":0, "pax":0, "countries": {}}
    tot_d = {"count":0, "pax":0, "countries": {}}
    daily = {}

    for i in range(days):
        dt = (datetime.datetime.fromisoformat(today_str) - datetime.timedelta(days=i)).date()
        dt_str = dt.isoformat()
        fp = Path(f"data/accumulated_{dt_str}.json")
        if not fp.exists(): continue
        try:
            d = json.loads(fp.read_text())
            a_count = d.get("arrivals", {}).get("count", 0)
            d_count = d.get("departures", {}).get("count", 0)
            a_ctry  = d.get("arrivals",   {}).get("countries", {})
            d_ctry  = d.get("departures", {}).get("countries", {})
            # Flatten country breakdown: {name: flights} (keep "country" meta for Russian cities)
            def flatten(ctry):
                out = {}
                for c, v in ctry.items():
                    out[c] = {"n": v.get("flights", 0)}
                    if "country" in v:
                        out[c]["country"] = v["country"]
                return out
            daily[dt_str] = {
                "arrivals":   a_count,
                "departures": d_count,
                "arrivals_by":   flatten(a_ctry),
                "departures_by": flatten(d_ctry),
            }
            for side, tot in [("arrivals", tot_a), ("departures", tot_d)]:
                x = d.get(side, {})
                tot["count"] += x.get("count", 0)
                tot["pax"] += x.get("pax", 0)
                for c, v in x.get("countries", {}).items():
                    if c not in tot["countries"]: tot["countries"][c] = {"flights": 0, "pax": 0}
                    tot["countries"][c]["flights"] += v.get("flights", 0)
                    tot["countries"][c]["pax"] += v.get("pax", 0)
                    if "country" in v:
                        tot["countries"][c]["country"] = v["country"]
        except Exception: continue
    return tot_a, tot_d, daily

def _merge_by(target, source):
    """Merge source {name: {n, ?country}} into target dict."""
    for c, v in source.items():
        n = v["n"] if isinstance(v, dict) else v
        if c not in target:
            target[c] = {"n": 0}
            if isinstance(v, dict) and "country" in v:
                target[c]["country"] = v["country"]
        target[c]["n"] += n

def make_by_days(daily):
    result = []
    for date_str in sorted(daily.keys()):
        dt = datetime.date.fromisoformat(date_str)
        label = f"{DAY_NAMES_RU[dt.weekday()]} {dt.day:02d}.{dt.month:02d}"
        result.append({
            "date": date_str, "label": label,
            "arrivals":   daily[date_str]["arrivals"],
            "departures": daily[date_str]["departures"],
            "arrivals_by":   daily[date_str].get("arrivals_by", {}),
            "departures_by": daily[date_str].get("departures_by", {}),
        })
    return result

def make_by_weeks(daily):
    weeks = {}
    for date_str in sorted(daily.keys()):
        dt = datetime.date.fromisoformat(date_str)
        iso_year, iso_week, _ = dt.isocalendar()
        key = f"{iso_year}-W{iso_week:02d}"
        if key not in weeks:
            weeks[key] = {"key": key,
                          "label": f"Нед {iso_week} ({MONTH_NAMES_RU[dt.month]})",
                          "arrivals": 0, "departures": 0,
                          "arrivals_by": {}, "departures_by": {}}
        weeks[key]["arrivals"]   += daily[date_str]["arrivals"]
        weeks[key]["departures"] += daily[date_str]["departures"]
        _merge_by(weeks[key]["arrivals_by"],   daily[date_str].get("arrivals_by", {}))
        _merge_by(weeks[key]["departures_by"], daily[date_str].get("departures_by", {}))
    return [v for _, v in sorted(weeks.items())]

def make_by_months(daily):
    months = {}
    for date_str in sorted(daily.keys()):
        dt = datetime.date.fromisoformat(date_str)
        key = f"{dt.year}-{dt.month:02d}"
        if key not in months:
            months[key] = {"key": key,
                           "label": f"{MONTH_NAMES_RU[dt.month]} {dt.year}",
                           "arrivals": 0, "departures": 0,
                           "arrivals_by": {}, "departures_by": {}}
        months[key]["arrivals"]   += daily[date_str]["arrivals"]
        months[key]["departures"] += daily[date_str]["departures"]
        _merge_by(months[key]["arrivals_by"],   daily[date_str].get("arrivals_by", {}))
        _merge_by(months[key]["departures_by"], daily[date_str].get("departures_by", {}))
    return [v for _, v in sorted(months.items())]

def save_to_supabase(date_str, a_acc, d_acc):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        payload = {
            "date": date_str,
            "arrivals_count": a_acc["count"],
            "arrivals_pax": a_acc["pax"],
            "departures_count": d_acc["count"],
            "departures_pax": d_acc["pax"],
            "arrivals_countries": a_acc["countries"],
            "departures_countries": d_acc["countries"],
            "updated_at": datetime.datetime.now(ICT).isoformat(),
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/flight_daily",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 201):
            print(f"✅ Supabase: {date_str} сохранён.")
        else:
            print(f"⚠️ Supabase error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ Supabase exception: {e}")

def fmt_top10(ctry):
    lines = []
    sorted_c = sorted(ctry.items(), key=lambda x: x[1]["flights"], reverse=True)[:10]
    for i, (c, v) in enumerate(sorted_c, 1):
        flag = COUNTRY_FLAGS.get(v.get("country", c), "")
        lines.append(f"{i}. {flag} {c}: {v['flights']} рейсов (~{v['pax']:,} пасс.)")
    return "\n".join(lines) if lines else "Нет данных"

def run():
    now = datetime.datetime.now(ICT)
    today = now.date().isoformat()
    today_date = now.date()
    tat_monthly = load_tat_stats()
    key, knum = get_api_key()

    a_fl, a_req = fetch_flights("arrival", key)
    d_fl, d_req = fetch_flights("departure", key)
    a_res = analyze(a_fl, "arrival")
    d_res = analyze(d_fl, "departure")

    a_cur = a_res.get(today, {"count":0, "pax":0, "countries":{}, "stats":{}})
    d_cur = d_res.get(today, {"count":0, "pax":0, "countries":{}, "stats":{}})

    # ── Accumulate today's data (merge, not overwrite) ──────────────────
    Path("data").mkdir(exist_ok=True)
    acc_file = Path(f"data/accumulated_{today}.json")
    acc = {"date": today,
           "arrivals":   {"count": 0, "pax": 0, "countries": {}},
           "departures": {"count": 0, "pax": 0, "countries": {}}}
    if acc_file.exists():
        try:
            acc = json.loads(acc_file.read_text())
        except Exception:
            pass

    for side_key, cur in [("arrivals", a_cur), ("departures", d_cur)]:
        side = acc.setdefault(side_key, {"count": 0, "pax": 0, "countries": {}})
        for c, v in cur["countries"].items():
            if c not in side["countries"]:
                side["countries"][c] = {"flights": 0, "pax": 0}
            side["countries"][c]["flights"] += v.get("flights", 0)
            side["countries"][c]["pax"]     += v.get("pax", 0)
            if "country" in v:
                side["countries"][c]["country"] = v["country"]
        side["count"] = sum(x["flights"] for x in side["countries"].values())
        side["pax"]   = sum(x["pax"]     for x in side["countries"].values())

    # Merge arrivals flight list — deduplicate code-shares by (from, arr_time)
    # Build lookup from current API data to patch missing airline/times on existing records
    cur_arr_by_fn = {r.get("fn"): r for r in a_cur.get("flight_list", []) if r.get("fn")}
    existing_arr = acc.get("arrivals_list", [])
    for r in existing_arr:
        fresh = cur_arr_by_fn.get(r.get("fn",""))
        if fresh:
            if not r.get("airline") and fresh.get("airline"): r["airline"] = fresh["airline"]
            if not r.get("arr_time") and fresh.get("arr_time"): r["arr_time"] = fresh["arr_time"]
            if not r.get("dep_time") and fresh.get("dep_time"): r["dep_time"] = fresh["dep_time"]
            r["status"] = fresh.get("status", r.get("status",""))
    seen_arr = {(r.get("from",""), r.get("arr_time","")) for r in existing_arr if r.get("arr_time")}
    seen_arr_fns = {r["fn"] for r in existing_arr if r.get("fn")}
    new_arr = []
    for r in a_cur.get("flight_list", []):
        key = (r.get("from",""), r.get("arr_time",""))
        if r.get("fn") in seen_arr_fns: continue
        if r.get("arr_time") and key in seen_arr: continue
        new_arr.append(r)
        seen_arr.add(key)
        seen_arr_fns.add(r.get("fn",""))
    acc["arrivals_list"] = existing_arr + new_arr

    # Auto-update stale arrivals: if arr_time > 45min ago → "landed"
    now_ict = datetime.datetime.now(ICT)
    for r in acc["arrivals_list"]:
        if r.get("status") in ("scheduled", "active") and r.get("arr_time"):
            try:
                arr_dt = datetime.datetime.fromisoformat(r["arr_time"])
                if arr_dt.tzinfo is None:
                    arr_dt = ICT.localize(arr_dt)
                if arr_dt < now_ict - datetime.timedelta(minutes=45):
                    r["status"] = "landed"
            except Exception:
                pass

    # Merge departures flight list — deduplicate code-shares by (to, dep_time)
    seen_dep = {(r.get("to",""), r.get("dep_time","")) for r in acc.get("departures_list", []) if r.get("dep_time")}
    seen_dep_fns = {r["fn"] for r in acc.get("departures_list", []) if r.get("fn")}
    new_dep = []
    for r in d_cur.get("flight_list", []):
        key = (r.get("to",""), r.get("dep_time",""))
        if r.get("fn") in seen_dep_fns: continue
        if r.get("dep_time") and key in seen_dep: continue
        new_dep.append(r)
        seen_dep.add(key)
        seen_dep_fns.add(r.get("fn",""))
    acc["departures_list"] = acc.get("departures_list", []) + new_dep

    # Auto-update stale "scheduled" departures: if dep_time > 45min ago → "departed"
    for r in acc["departures_list"]:
        if r.get("status") == "scheduled" and r.get("dep_time"):
            try:
                dep_dt = datetime.datetime.fromisoformat(r["dep_time"])
                if dep_dt.tzinfo is None:
                    dep_dt = ICT.localize(dep_dt)
                if dep_dt < now_ict - datetime.timedelta(minutes=45):
                    r["status"] = "departed"
            except Exception:
                pass

    acc_file.write_text(json.dumps(acc, indent=2, ensure_ascii=False))

    # Persist to Supabase
    save_to_supabase(today, acc.get("arrivals", {}), acc.get("departures", {}))

    # Read accumulated today for dashboard
    a_acc = acc.get("arrivals",   {"count": 0, "pax": 0, "countries": {}})
    d_acc = acc.get("departures", {"count": 0, "pax": 0, "countries": {}})

    # ── Period stats ─────────────────────────────────────────────────────
    w_a, w_d, w_daily = load_period_stats(today, 7)
    m_a, m_d, m_daily = load_period_stats(today, 30)
    q_a, q_d, q_daily = load_period_stats(today, 90)
    h_a, h_d, h_daily = load_period_stats(today, 180)
    y_a, y_d, y_daily = load_period_stats(today, 365)

    # ── Yesterday ────────────────────────────────────────────────────────
    yesterday = (datetime.datetime.fromisoformat(today) - datetime.timedelta(days=1)).date().isoformat()
    yest_file = Path(f"data/accumulated_{yesterday}.json")
    if yest_file.exists():
        yest_data = json.loads(yest_file.read_text())
        v_a = yest_data.get("arrivals",   {"count":0, "pax":0, "countries":{}})
        v_d = yest_data.get("departures", {"count":0, "pax":0, "countries":{}})
        v_arrivals_list   = yest_data.get("arrivals_list",   [])
        v_departures_list = yest_data.get("departures_list", [])
    else:
        v_a = {"count":0, "pax":0, "countries":{}}
        v_d = {"count":0, "pax":0, "countries":{}}
        v_arrivals_list   = []
        v_departures_list = []

    # ── Russian transit estimates ─────────────────────────────────────────
    yest_date = today_date - datetime.timedelta(days=1)
    rt_today     = calc_russian_transit(1,   a_acc["countries"], tat_monthly, today_date)
    rt_yesterday = calc_russian_transit(1,   v_a["countries"],   tat_monthly, yest_date)
    rt_week      = calc_russian_transit(7,   w_a["countries"],   tat_monthly, today_date)
    rt_month     = calc_russian_transit(30,  m_a["countries"],   tat_monthly, today_date)
    rt_quarter   = calc_russian_transit(90,  q_a["countries"],   tat_monthly, today_date)
    rt_halfyear  = calc_russian_transit(180, h_a["countries"],   tat_monthly, today_date)
    rt_year      = calc_russian_transit(365, y_a["countries"],   tat_monthly, today_date)

    # ── Telegram ─────────────────────────────────────────────────────────
    def block(title, a, d):
        lines = [f"<b>{title}</b>",
                 f"✈️ Всего: {a['count']+d['count']} | 👥 ~{a['pax']+d['pax']:,}",
                 "<b>🛬 ПРИЛЁТЫ (Топ-10)</b>", fmt_top10(a['countries']),
                 "<b>🛫 ВЫЛЕТЫ (Топ-10)</b>", fmt_top10(d['countries']), ""]
        return "\n".join(lines)

    msg = f"🌴 <b>PHUKET HKT STATISTICS</b>\n🕒 {now.strftime('%H:%M')} | API #{knum}\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += block("🔴 СЕГОДНЯ (накопл.)", a_acc, d_acc)
    msg += block("📊 НЕДЕЛЯ (7 дней)", w_a, w_d)
    msg += block("📈 МЕСЯЦ (30 дней)", m_a, m_d)
    msg += "📍 <i>HKT | Phuket International Airport</i>"

    print(msg)
    send_telegram(msg)

    # ── Dashboard JSON ────────────────────────────────────────────────────
    def fmt_all(ctry):
        sorted_c = sorted(ctry.items(), key=lambda x: x[1]["flights"], reverse=True)
        result = []
        for c, v in sorted_c:
            item = {"name": c, "flights": v["flights"], "pax": v["pax"]}
            if "country" in v:
                item["country"] = v["country"]
            result.append(item)
        return result

    def to_web_fmt(arr, dep, breakdown=None, russia_transit=None, arrivals_list=None, departures_list=None):
        d = {
            "arrivals":   {"count": arr["count"], "pax": arr["pax"], "all": fmt_all(arr["countries"])},
            "departures": {"count": dep["count"], "pax": dep["pax"], "all": fmt_all(dep["countries"])},
        }
        if breakdown:
            d.update(breakdown)
        if russia_transit and russia_transit.get("pax", 0) > 0:
            d["russia_transit"] = russia_transit
        if arrivals_list:
            d["arrivals_list"] = arrivals_list
        if departures_list:
            d["departures_list"] = departures_list
        return d

    dashboard_data = {
        "updated":   now.isoformat(),
        "yesterday": to_web_fmt(v_a,  v_d,  russia_transit=rt_yesterday, arrivals_list=v_arrivals_list, departures_list=v_departures_list),
        "today":     to_web_fmt(a_acc, d_acc, russia_transit=rt_today,     arrivals_list=acc.get("arrivals_list", []), departures_list=acc.get("departures_list", [])),
        "week":      to_web_fmt(w_a,  w_d,  {"by_days":   make_by_days(w_daily)},   russia_transit=rt_week),
        "month":     to_web_fmt(m_a,  m_d,  {"by_weeks":  make_by_weeks(m_daily)},  russia_transit=rt_month),
        "quarter":   to_web_fmt(q_a,  q_d,  {"by_months": make_by_months(q_daily)}, russia_transit=rt_quarter),
        "halfyear":  to_web_fmt(h_a,  h_d,  {"by_months": make_by_months(h_daily)}, russia_transit=rt_halfyear),
        "year":      to_web_fmt(y_a,  y_d,  {"by_months": make_by_months(y_daily)}, russia_transit=rt_year),
    }
    Path("data/dashboard.json").write_text(json.dumps(dashboard_data, ensure_ascii=False, indent=2))
    print("✅ dashboard.json обновлён.")

if __name__ == "__main__":
    run()

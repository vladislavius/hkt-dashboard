import requests, json, datetime, pytz
from pathlib import Path
from collections import Counter

API_KEYS = [
    "07deba653b969abc059382a133349e8f",
    "a80602f218f99c940df4bb320fd700da",
    "c58ee63777c13244220188930f692f85"
]
TG_TOKEN = "8214283336:AAFSKaaWdXJjWDb2o1xh_zemAy4dqg_CHv4"
TG_CHAT_ID = "153282547"

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
    'TAS':'Uzbekistan','GYD':'Azerbaijan','EVN':'Armenia','FRU':'Kyrgyzstan','DYU':'Tajikistan','ASB':'Turkmenistan',
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

CAP = {'319':140,'320':180,'321':220,'332':280,'333':320,'339':350,'359':325,'388':525,'737':180,'738':189,'739':220,'73J':220,'772':314,'773':365,'77W':396,'77L':314,'788':250,'789':290,'78J':330,'32A':180,'32B':186,'32N':186,'32Q':186,'E90':100,'E95':120,'CR9':90,'DH4':78,'AT7':70,'CRJ':90,'32S':180}

ICT = pytz.timezone('Asia/Bangkok')

def get_api_key():
    now = datetime.datetime.now(ICT)
    h = now.hour
    if h < 4: return API_KEYS[0], 1
    elif h < 12: return API_KEYS[1], 2
    elif h < 20: return API_KEYS[2], 3
    else: return API_KEYS[0], 1

def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", 
                      json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception: pass

def fetch_flights(direction, api_key):
    key = "arr_iata" if direction == "arrival" else "dep_iata"
    try:
        r = requests.get(BASE, params={"access_key": api_key, key: "HKT", "limit": 100}, timeout=20)
        r.raise_for_status()
        return r.json().get("data", []), 1
    except Exception: return [], 0

def analyze(flights, direction):
    by_date = {}
    for f in flights:
        d = f.get("flight_date")
        if d: by_date.setdefault(d, []).append(f)
    res = {}
    for date, fl in by_date.items():
        cnt, pax, ctry, st = 0, 0, Counter(), {"completed":0,"upcoming":0,"cancelled":0}
        for f in fl:
            dep = (f.get("departure") or {}).get("iata", "")
            arr = (f.get("arrival") or {}).get("iata", "")
            if (direction=="arrival" and dep in DOMESTIC) or (direction=="departure" and arr in DOMESTIC): continue
            cnt += 1
            pax += CAP.get((f.get("aircraft") or {}).get("iata", ""), 180)
            ctry[MAP_C.get(dep if direction=="arrival" else arr, f"Other({dep if direction=='arrival' else arr})")] += 1
            s = (f.get("flight_status") or "").lower().strip()
            if direction == "departure":
                if s in ("departed","landed","diverted","active","en route","en-route","incidents","taxi-out","pushback","returned","taxi"): st["completed"]+=1
                elif s in ("scheduled","taxiing","boarding","expected","estimated","gate","holding"): st["upcoming"]+=1
                elif s == "cancelled": st["cancelled"]+=1
            else:
                if s in ("landed","arrived","diverted"): st["completed"]+=1
                elif s in ("scheduled","active","en route","en-route","taxiing","taxi","boarding","expected","estimated"): st["upcoming"]+=1
                elif s == "cancelled": st["cancelled"]+=1
        res[date] = {"count": cnt, "pax": pax, "countries": dict(ctry), "stats": st}
    return res

def load_period_stats(today_str, days):
    tot_a = {"count":0, "pax":0, "countries": Counter()}
    tot_d = {"count":0, "pax":0, "countries": Counter()}
    for i in range(days):
        dt = (datetime.datetime.fromisoformat(today_str) - datetime.timedelta(days=i)).date().isoformat()
        fp = Path(f"data/accumulated_{dt}.json")
        if not fp.exists(): continue
        try:
            d = json.loads(fp.read_text())
            for side, tot in [("arrivals", tot_a), ("departures", tot_d)]:
                x = d.get(side, {})
                tot["count"] += x.get("count", 0)
                tot["pax"] += x.get("pax", 0)
                tot["countries"].update(x.get("countries", {}))
        except Exception: continue
    return tot_a, tot_d

def fmt_top10(ctry):
    lines = []
    for i, (c, v) in enumerate(sorted(ctry.items(), key=lambda x: x[1], reverse=True)[:10], 1):
        lines.append(f"{i}. {COUNTRY_FLAGS.get(c,'')} {c}: {v}")
    return "\n".join(lines) if lines else "Нет данных"

def run():
    now = datetime.datetime.now(ICT)
    today = now.date().isoformat()
    key, knum = get_api_key()
    
    a_fl, a_req = fetch_flights("arrival", key)
    d_fl, d_req = fetch_flights("departure", key)
    a_res = analyze(a_fl, "arrival")
    d_res = analyze(d_fl, "departure")
    
    a_cur = a_res.get(today, {"count":0, "pax":0, "countries":{}, "stats":{}})
    d_cur = d_res.get(today, {"count":0, "pax":0, "countries":{}, "stats":{}})
    
    Path("data").mkdir(exist_ok=True)
    Path(f"data/accumulated_{today}.json").write_text(json.dumps({
        "date": today,
        "arrivals": {"count": a_cur["count"], "pax": a_cur["pax"], "countries": a_cur["countries"]},
        "departures": {"count": d_cur["count"], "pax": d_cur["pax"], "countries": d_cur["countries"]}
    }, indent=2, ensure_ascii=False))
    
    w_a, w_d = load_period_stats(today, 7)
    m_a, m_d = load_period_stats(today, 30)
    
    def block(title, a, d):
        lines = [f"<b>{title}</b>", f"✈️ Всего: {a['count']+d['count']} | 👥 ~{a['pax']+d['pax']:,}", 
                 "<b>🛬 ПРИЛЁТЫ (Топ-10)</b>", fmt_top10(a['countries']),
                 "<b>🛫 ВЫЛЕТЫ (Топ-10)</b>", fmt_top10(d['countries']), ""]
        return "\n".join(lines)

    msg = f"🌴 <b>PHUKET HKT STATISTICS</b>\n🕒 {now.strftime('%H:%M')} | API #{knum}\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += block("🔴 СЕГОДНЯ", a_cur, d_cur)
    msg += block("📊 НЕДЕЛЯ (7 дней)", w_a, w_d)
    msg += block("📈 МЕСЯЦ (30 дней)", m_a, m_d)
    msg += "📍 <i>HKT | Phuket International Airport</i>"
    
    print(msg)
    send_telegram(msg)


def export_dashboard(today_str, a_cur, d_cur, w_a, w_d, m_a, m_d):
    def fmt(ctry):
        return [{"name": c, "count": v} for c, v in sorted(ctry.items(), key=lambda x: x[1], reverse=True)[:10]]
    data = {
        "updated": datetime.datetime.now(pytz.timezone('Asia/Bangkok')).isoformat(),
        "today": {"arrivals": {"count": a_cur["count"], "pax": a_cur["pax"], "top": fmt(a_cur["countries"])},
                  "departures": {"count": d_cur["count"], "pax": d_cur["pax"], "top": fmt(d_cur["countries"])}},
        "week": {"arrivals": {"count": w_a["count"], "pax": w_a["pax"], "top": fmt(w_a["countries"])},
                 "departures": {"count": w_d["count"], "pax": w_d["pax"], "top": fmt(w_d["countries"])}},
        "month": {"arrivals": {"count": m_a["count"], "pax": m_a["pax"], "top": fmt(m_a["countries"])},
                  "departures": {"count": m_d["count"], "pax": m_d["pax"], "top": fmt(m_d["countries"])}}
    }
    Path("data/dashboard.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    run()

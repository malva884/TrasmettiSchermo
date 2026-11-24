"""
Sistema Centralizzato di Digital Signage - Server
Flask + Flask-SocketIO (rooms), persistenza su config.json, media in /media.
"""
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from functools import wraps

import requests as ext_requests
from flask import (Flask, Response, abort, jsonify, make_response,
                   render_template, request, send_from_directory)
from flask_socketio import SocketIO, join_room, leave_room
from urllib.parse import urlparse, quote
from werkzeug.utils import secure_filename

try:
    # Europe/Rome e' l'ora di riferimento del palinsesto; su host senza
    # tzdata di sistema (Windows dev) serve il pacchetto 'tzdata'.
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo(os.environ.get("TZ", "Europe/Rome"))
except Exception:
    LOCAL_TZ = timezone.utc
    print("WARN: tzdata non disponibile, palinsesto valutato in UTC. "
          "Installare il pacchetto 'tzdata' o impostare TZ.")

try:
    # Accesso autenticato a Google Sheets via Service Account (task board)
    from google.oauth2 import service_account as _sa_module
    from google.auth.transport.requests import Request as _ga_request
    _GOOGLE_AUTH_OK = True
except Exception:
    _sa_module = None
    _ga_request = None
    _GOOGLE_AUTH_OK = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Percorsi persistenti configurabili via env (volumi Coolify/Docker).
# In produzione: DATA_DIR=/app/data, MEDIA_DIR=/app/static/media.
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
MEDIA_DIR = os.environ.get("MEDIA_DIR", os.path.join(BASE_DIR, "media"))
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")

# Retro-compatibilita': senza DATA_DIR, se esiste un config.json nella
# root del progetto (setup pre-Coolify) continua a usarlo.
if not os.environ.get("DATA_DIR") and not os.path.exists(CONFIG_PATH):
    _legacy = os.path.join(BASE_DIR, "config.json")
    if os.path.exists(_legacy):
        CONFIG_PATH = _legacy

VIDEO_EXT = {"mp4", "webm", "mov", "m4v"}
IMAGE_EXT = {"jpg", "jpeg", "png", "webp", "gif", "avif", "svg"}
ALLOWED_EXTENSIONS = VIDEO_EXT | IMAGE_EXT

# URL YouTube riconosciuti: watch?v=, youtu.be/, /embed/, /shorts/, /live/,
# /v/, /playlist?list= e qualsiasi host youtube*/youtu.be (anche music.).
YOUTUBE_RE = re.compile(r"(?:youtube(?:-nocookie)?\.com|youtu\.be)",
                        re.IGNORECASE)
_YT_ID_RE = re.compile(
    r"(?:youtube(?:-nocookie)?\.com/(?:watch\?[^#]*v=|embed/|shorts/|live/|v/)"
    r"|youtu\.be/)([\w-]{6,})", re.IGNORECASE)

ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")

DEFAULT_CONTENT_URL = os.environ.get("DEFAULT_CONTENT_URL", "")

app = Flask(__name__)
app.jinja_env.auto_reload = True  # template modificati serviti subito
app.config["MAX_CONTENT_LENGTH"] = 250 * 1024 * 1024  # 250 MB max upload

# async_mode: gevent in produzione (Gunicorn GeventWebSocketWorker),
# threading in dev locale se gevent non e' installato.
try:
    import gevent  # noqa: F401
    _ASYNC_MODE = "gevent"
except ImportError:
    _ASYNC_MODE = "threading"

socketio = SocketIO(app, async_mode=_ASYNC_MODE, cors_allowed_origins="*",
                    ping_interval=25, ping_timeout=60)

# Dirette News TV: sorgenti di default, sovrascrivibili dall'admin
# (state['news_tv'] -> config.json) se l'emittente cambia indirizzo.
NEWS_TV_DEFAULTS = {
    # Sky TG24: playlist HLS ufficiale (la stessa del player web di
    # tg24.sky.it), riprodotta client-side via hls.js: niente blocchi
    # di incorporamento YouTube ne' ID diretta che cambiano. Il token
    # hdnts scade a dicembre 2027: se lo stream smette di andare,
    # aggiornare la sorgente in App & Widget -> Diretta News TV con un
    # nuovo URL master.m3u8 (variante /TACT/ firmata o /OFF/ tokenless).
    # In alternativa la sorgente accetta anche ID video/canale YouTube
    # — vedi news_tv_yt_src.
    "skytg24": "https://hlslive-web-gcdn-skycdn-it.akamaized.net/"
               "TACT/12221/web/master.m3u8"
               "?hdnts=st=1764666351~exp=1829466206~acl=/*"
               "~hmac=b0e9165b6c55027903ad103c8219f363d8765eb300c0d9a339e976"
               "7fc3509556",
    # Rai News 24: playlist HLS ufficiale CDN redge.media, riprodotta
    # client-side (hls.js) cosi' usa l'IP italiano della sede
    "rainews24": "https://n-5-1.eu.cdn-redge.media/livehls/oeu/mt/live/"
                 "rai_news_24/live.livx/playlist.m3u8",
}

state_lock = threading.RLock()
state = {"gruppi": {}, "dispositivi": {}, "app_configs": {},
         "news_tv": dict(NEWS_TV_DEFAULTS), "schedules": []}
clients = {}            # gruppo -> set di sid connessi
sid_group = {}          # sid -> gruppo
# Runtime del palinsesto (NON persistito in config.json):
# sched_active: 'g:<gruppo>'/'d:<dev>' -> fascia in onda come overlay
#               {sched_id,url,tipo,mode}. Solo eventi Socket.IO: i campi
#               persistenti (contenuto_attivo / dev.contenuto) non vengono
#               mai toccati dallo scheduler.
# sched_manual: stessa chiave -> contenuto messo in onda a mano: vince il
#               palinsesto fino al cambio fascia (o 'Riprendi palinsesto')
sched_active = {}
sched_manual = {}
device_sids = {}        # device_id -> set di sid connessi
sid_device = {}         # sid -> device_id
playlist_workers = {}   # gruppo -> {"stop": Event, "thread": Thread}
weather_cache = {}      # (lat, lon) -> {"ts": epoch, "data": payload}

UNASSIGNED = "non_assegnato"
DEFAULT_GROUP = "default"  # gruppo di default per device auto-registrati/orfani
YT_SLIDE_TIMEOUT = 24 * 3600  # cap slide YouTube con durata_sec=0 (a fine video)


# ---------------------------------------------------------------- utils

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_config():
    global state
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("gruppi"), dict):
                state = data
                state.setdefault("dispositivi", {})
                state.setdefault("app_configs", {})
                state.setdefault("schedules", [])
                _nt = dict(NEWS_TV_DEFAULTS)
                _nt.update(state.get("news_tv") or {})
                state["news_tv"] = _nt
        except (json.JSONDecodeError, OSError):
            pass


def save_config():
    # serializzata: scritture concorrenti (playlist worker + handler HTTP)
    # sullo stesso .tmp farebbero fallire os.replace su Windows.
    # state_lock e' RLock: sicuro anche quando chiamata dentro il lock.
    with state_lock:
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CONFIG_PATH)


def yt_parse(url):
    """Estrae (video_id, list_id) da un URL YouTube, o (None, None)."""
    u = url or ""
    m = _YT_ID_RE.search(u)
    video_id = m.group(1) if m else None
    m = re.search(r"[?&]list=([\w-]+)", u)
    list_id = m.group(1) if m else None
    return video_id, list_id


def yt_embed_src(url, autoplay=True):
    """URL embed HTTPS (youtube-nocookie) per video/playlist YouTube.
    Per i video singoli imposta playlist=<ID>: requisito obbligatorio delle
    API Embed di YouTube affinche' loop=1 funzioni."""
    vid, lst = yt_parse(url)
    params = "rel=0&playsinline=1&loop=1"
    if autoplay:
        params = "autoplay=1&mute=1&" + params
    base = "https://www.youtube-nocookie.com/embed/"
    if lst:
        # playlist YouTube: loop=1 cicla l'intera lista
        path = vid or "videoseries"
        return f"{base}{path}?{params}&list={lst}"
    if vid:
        return f"{base}{vid}?{params}&playlist={vid}"
    return None


def news_tv_yt_src(value, mute=True):
    """URL embed youtube-nocookie per la diretta Sky TG24.
    Accetta: ID video, ID canale UC… (-> embed/live_stream, segue sempre
    la diretta corrente), URL watch/embed o URL live_stream?channel=.
    Ritorna None se il valore non e' riconoscibile."""
    p = (f"autoplay=1&mute={1 if mute else 0}&controls=0"
         f"&modestbranding=1&rel=0&playsinline=1")
    v = (value or "").strip()
    if not v:
        return None
    if re.fullmatch(r"UC[\w-]{20,}", v):
        return ("https://www.youtube-nocookie.com/embed/live_stream"
                f"?channel={v}&{p}")
    if v.startswith("http"):
        m = re.search(r"[?&]channel=([\w-]+)", v)
        if "live_stream" in v and m:
            return ("https://www.youtube-nocookie.com/embed/live_stream"
                    f"?channel={m.group(1)}&{p}")
        vid, _ = yt_parse(v)
        v = vid or ""
    if not re.fullmatch(r"[\w-]{6,}", v):
        return None
    return f"https://www.youtube-nocookie.com/embed/{v}?{p}"


def detect_tipo(url):
    u = (url or "").split("?")[0].lower()
    ext = u.rsplit(".", 1)[-1] if "." in u else ""
    if ext in VIDEO_EXT:
        return "video"
    if ext in IMAGE_EXT:
        return "image"
    if url and YOUTUBE_RE.search(url):
        return "youtube"
    if url:
        return "url"
    return "idle"


VALID_MODES = {"auto", "iframe", "proxy", "redirect"}
PROXY_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def check_frameable(url):
    """True se l'URL può essere caricato in un iframe cross-origin.
    Ispeziona X-Frame-Options e CSP frame-ancestors lato server."""
    try:
        r = ext_requests.head(url, timeout=8, allow_redirects=True,
                              headers={"User-Agent": PROXY_UA})
        if r.status_code >= 400 or r.status_code == 405:
            r = ext_requests.get(url, timeout=8, stream=True,
                                 headers={"User-Agent": PROXY_UA})
            r.close()
    except ext_requests.RequestException:
        return True  # irraggiungibile dal server: lascia iframe (es. intranet)
    xfo = (r.headers.get("X-Frame-Options") or "").lower()
    if xfo and "allowall" not in xfo:
        return False
    csp = r.headers.get("Content-Security-Policy") or ""
    m = re.search(r"frame-ancestors\s+([^;]+)", csp, re.I)
    if m:
        val = m.group(1)
        if "'none'" in val:
            return False
        if "*" not in val and request.host not in val and "http" not in val:
            return False  # whitelist esplicita che non ci include
    return True


def allowed_proxy_urls():
    """URL esterni proxy-abili: solo quelli configurati nei gruppi/dispositivi."""
    with state_lock:
        urls = set()
        for g in state["gruppi"].values():
            if g.get("contenuto_attivo"):
                urls.add(g["contenuto_attivo"])
            for it in g.get("playlist") or []:
                if it.get("url"):
                    urls.add(it["url"])
        for d in state.get("dispositivi", {}).values():
            c = d.get("contenuto") or {}
            if c.get("url"):
                urls.add(c["url"])
        return urls


def cache_busted(url):
    """Appende un cache-buster ai media locali per evitare cache stale."""
    if url and url.startswith("/media/"):
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}v={int(time.time())}"
    return url


def content_payload(g):
    url = g.get("contenuto_attivo", "")
    return {
        "url": cache_busted(url),
        "url_raw": url,
        "tipo": g.get("tipo", detect_tipo(url)),
        "mode": g.get("mode", "iframe"),
        "ultimo_aggiornamento": g.get("ultimo_aggiornamento"),
        "playlist_attiva": bool(g.get("playlist")),
    }


def get_or_create_gruppo(nome):
    with state_lock:
        if nome not in state["gruppi"]:
            state["gruppi"][nome] = {
                "contenuto_attivo": DEFAULT_CONTENT_URL,
                "tipo": detect_tipo(DEFAULT_CONTENT_URL),
                "mode": "iframe",
                "ultimo_aggiornamento": now_iso(),
                "playlist": [],
            }
            save_config()
        return state["gruppi"][nome]


def register_device(dev_id, gruppo_url=None, user_agent=None):
    """Crea/aggiorna il record dispositivo. Il gruppo nell'URL vale solo
    come default alla prima registrazione: le riassegnazioni da admin
    sono autorevoli e sopravvivono ai reboot del client.
    Device nuovo -> gruppo 'default' (auto-creato), nome = device_id."""
    with state_lock:
        devs = state.setdefault("dispositivi", {})
        if dev_id not in devs:
            gruppo = gruppo_url or DEFAULT_GROUP
            if gruppo != UNASSIGNED:
                get_or_create_gruppo(gruppo)
            devs[dev_id] = {
                "nome": dev_id,
                "gruppo": gruppo,
                "user_agent": user_agent or "",
                "contenuto": None,  # override dedicato (Trasmetti solo qui)
                "primo_visto": now_iso(),
            }
        devs[dev_id]["ultimo_visto"] = now_iso()
        if user_agent:
            devs[dev_id]["user_agent"] = user_agent[:300]
        dev = dict(devs[dev_id])
    save_config()
    return dev


def idle_payload():
    return {"url": "", "url_raw": "", "tipo": "idle", "mode": "iframe",
            "ultimo_aggiornamento": None, "playlist_attiva": False}


def emit_to_device(dev_id):
    """Invia al singolo device il contenuto attivo: override dedicato
    (contenuto proprio) oppure il contenuto del gruppo assegnato."""
    with state_lock:
        dev = dict(state["dispositivi"].get(dev_id) or {})
        own = dev.get("contenuto") or {}
        gname = dev.get("gruppo", UNASSIGNED)
        g = state["gruppi"].get(gname)
        ov_dev = sched_active.get("d:" + dev_id)
        ov_grp = sched_active.get("g:" + gname)
    if own.get("tipo") in ("idle", "none") and not own.get("url"):
        # override "schermata pairing": il device mostra la schermata
        # d'attesa con il suo codice, isolato dal feed del gruppo
        payload = idle_payload()
    elif ov_dev:
        # fascia palinsesto sul device: vince anche su contenuto dedicato
        # rimasto in stato (il manuale scade al cambio fascia)
        payload = _sched_payload(ov_dev)
    elif own.get("url"):
        url = own["url"]
        payload = {
            "url": cache_busted(url),
            "url_raw": url,
            "tipo": own.get("tipo") or detect_tipo(url),
            "mode": own.get("mode", "iframe"),
            "ultimo_aggiornamento": own.get("ultimo_aggiornamento"),
            "playlist_attiva": False,
        }
    elif ov_grp:
        # fascia palinsesto sul gruppo del device
        payload = _sched_payload(ov_grp)
    elif g:
        payload = gruppo_base_payload(g)
    else:
        payload = idle_payload()
    payload["device_id"] = dev_id
    payload["device_nome"] = dev.get("nome") or dev_id
    payload["gruppo"] = gname
    payload["dedicato"] = bool(own) or bool(ov_dev)
    socketio.emit("content", payload, to=f"dev:{dev_id}")


def move_device_room(dev_id, old_group, new_group):
    """Sposta i sid del device dalla room del vecchio gruppo a quella nuova."""
    sids = set(device_sids.get(dev_id) or set())
    for sid in sids:
        if old_group and old_group != UNASSIGNED:
            try:
                leave_room(old_group, sid=sid, namespace='/')
            except Exception:
                pass
            if old_group in clients:
                clients[old_group].discard(sid)
                if not clients[old_group]:
                    del clients[old_group]
            sid_group.pop(sid, None)
        if new_group and new_group != UNASSIGNED:
            join_room(new_group, sid=sid, namespace='/')
            sid_group[sid] = new_group
            clients.setdefault(new_group, set()).add(sid)


def build_stato():
    with state_lock:
        media = sorted(f for f in os.listdir(MEDIA_DIR)
                       if not f.startswith(".")) if os.path.isdir(MEDIA_DIR) else []
        return {
            "gruppi": {k: dict(v) for k, v in state["gruppi"].items()},
            "clients": {k: len(v) for k, v in clients.items()},
            "dispositivi": {
                k: dict(v, online=bool(device_sids.get(k)))
                for k, v in state.get("dispositivi", {}).items()
            },
            "media": media,
            "app_configs": {k: dict(v)
                            for k, v in state.get("app_configs", {}).items()},
            "news_tv": dict(state.get("news_tv") or NEWS_TV_DEFAULTS),
            "schedules": [dict(s) for s in state.get("schedules") or []],
            "sched_active": {k: dict(v) for k, v in sched_active.items()},
            "sched_manual": sorted(sched_manual),
            "server_time": now_iso(),
        }


def broadcast_stato():
    socketio.emit("stato", build_stato())


def stop_playlist(nome):
    w = playlist_workers.pop(nome, None)
    if w:
        w["stop"].set()
        w["advance"].set()  # sveglia il worker fermo in wait()


def start_playlist(nome):
    stop_playlist(nome)
    ev = threading.Event()
    adv = threading.Event()
    t = threading.Thread(target=playlist_worker,
                         args=(nome, ev, adv), daemon=True)
    playlist_workers[nome] = {"stop": ev, "advance": adv, "thread": t,
                              "slide_ts": 0.0}
    t.start()


def playlist_worker(nome, stop_ev, advance_ev):
    idx = 0
    while not stop_ev.is_set():
        if sched_active.get("g:" + nome):
            # fascia palinsesto in onda: la playlist resta in pausa e
            # riprende da dove era quando la fascia termina
            advance_ev.wait(5)
            advance_ev.clear()
            continue
        with state_lock:
            g = state["gruppi"].get(nome)
            items = (g or {}).get("playlist") or []
            if not items:
                break
            item = items[idx % len(items)]
            url = item.get("url", "")
            tipo = detect_tipo(url)
            try:
                durata = int(item.get("durata_sec", 15))
            except (TypeError, ValueError):
                durata = 15
            if durata <= 0:
                # YouTube con durata 0 = "a fine video": il player segnala
                # l'ENDED via WS (playlist_next); cap di sicurezza 24h.
                durata = YT_SLIDE_TIMEOUT if tipo == "youtube" else 15
            g["contenuto_attivo"] = url
            g["tipo"] = tipo
            g["ultimo_aggiornamento"] = now_iso()
            payload = content_payload(g)
            payload["durata_sec"] = durata
            w = playlist_workers.get(nome)
            if w:
                w["slide_ts"] = time.time()
        socketio.emit("content", payload, to=nome)
        save_config()
        broadcast_stato()
        idx += 1
        # attende durata_sec O il segnale del player (fine video YT)
        advance_ev.wait(durata)
        advance_ev.clear()


def emit_content(nome):
    g = get_or_create_gruppo(nome)
    ov = sched_active.get("g:" + nome)
    socketio.emit("content",
                  _sched_payload(ov) if ov else gruppo_base_payload(g),
                  to=nome)


# ---------------------------------------------------------------- Palinsesto
# Fasce orarie in state['schedules'] (config.json). Un worker in background
# valuta ogni 30s le regole attive (fuso Europe/Rome) e manda in onda il
# contenuto previsto. Regole:
# - La fascia e' un OVERLAY runtime (sched_active): lo scheduler emette
#   solo eventi Socket.IO 'content' e non scrive MAI contenuto_attivo /
#   dev.contenuto / default_content, che restano il contenuto base
#   impostato dall'operatore e sopravvivono al reboot.
# - 'Metti in onda' manuale marca sched_manual e vince finche' non cambia
#   la fascia attiva (o fino a 'Riprendi palinsesto').
# - A fine fascia: ripristina il contenuto base del gruppo
#   (contenuto_attivo -> default_content -> attesa) o riporta il device
#   nel feed del suo gruppo.


def _sched_payload(ov):
    """Payload 'content' per una fascia del palinsesto in onda."""
    url = ov.get("url") or ""
    return {"url": cache_busted(url), "url_raw": url,
            "tipo": ov.get("tipo") or detect_tipo(url),
            "mode": ov.get("mode") or "iframe",
            "ultimo_aggiornamento": now_iso(),
            "playlist_attiva": False,
            "sched_id": ov.get("sched_id")}


def gruppo_base_payload(g):
    """Payload del contenuto base del gruppo (quello persistito):
    contenuto_attivo -> default_content -> attesa (url vuoto)."""
    if (g.get("contenuto_attivo") or "").strip():
        return content_payload(g)
    d = g.get("default_content") or {}
    if d.get("url"):
        url = d["url"]
        return {"url": cache_busted(url), "url_raw": url,
                "tipo": d.get("tipo") or detect_tipo(url),
                "mode": d.get("mode") or "iframe",
                "ultimo_aggiornamento": g.get("ultimo_aggiornamento"),
                "playlist_attiva": False}
    return content_payload(g)


def _sched_emit_target(key):
    """Emette lo stato corrente di un target (overlay palinsesto o base).
    Per i device gestisce anche l'isolamento dalla room del gruppo."""
    if key.startswith("d:"):
        dev_id = key[2:]
        with state_lock:
            dev = state["dispositivi"].get(dev_id)
            if not dev:
                sched_active.pop(key, None)
                return
            gname = dev.get("gruppo", UNASSIGNED)
        if sched_active.get(key):
            move_device_room(dev_id, gname, None)   # isola dal feed di gruppo
        else:
            move_device_room(dev_id, None, gname)   # rientra nel feed
        emit_to_device(dev_id)
    else:
        with state_lock:
            if key[2:] not in state["gruppi"]:
                sched_active.pop(key, None)
                return
        emit_content(key[2:])


def sched_tick():
    """Una valutazione del palinsesto: calcola la fascia 'voluta' per ogni
    target e applica solo i cambi rispetto a sched_active. Nessuna
    scrittura sui campi persistenti del gruppo/device."""
    now = datetime.now(LOCAL_TZ)
    hhmm = now.strftime("%H:%M")
    day = now.isoweekday()          # 1 = Lunedi' .. 7 = Domenica
    with state_lock:
        scheds = [dict(s) for s in state.get("schedules") or []
                  if s.get("enabled")]
        gruppi = list(state["gruppi"])
    desired = {}                    # 'g:<g>'/'d:<d>' -> fascia attiva
    for s in scheds:
        if day not in (s.get("days") or []):
            continue
        if not (s.get("start_time", "00:00") <= hhmm
                < (s.get("end_time") or "23:59")):
            continue
        t = s.get("target") or ""
        if t == "all":
            keys = ["g:" + n for n in gruppi]
        elif t.startswith("device:"):
            keys = ["d:" + t[7:]]
        else:
            keys = ["g:" + t[6:] if t.startswith("group:") else "g:" + t]
        for k in keys:
            desired.setdefault(k, s)   # in caso di overlap vince la prima regola
    with state_lock:
        cands = set(desired) | set(sched_active) | set(sched_manual)
    applied = False
    for key in cands:
        s = desired.get(key)
        want = s["id"] if s else "base"
        if key in sched_manual:
            mref = sched_manual[key]
            if mref is True:
                # primo tick dopo l'intervento manuale: registra lo stato
                # che il palinsesto vuole ORA; finche' non cambia il
                # contenuto manuale resta in onda
                sched_manual[key] = want
                continue
            if want == mref:
                continue             # stessa fascia/stato: manuale resta
            sched_manual.pop(key, None)   # fascia cambiata: manuale scade
        cur = sched_active.get(key)
        last = cur["sched_id"] if cur else "base"
        if want == last:
            continue                 # niente da cambiare
        if s:
            sched_active[key] = {"sched_id": s["id"],
                                 "url": s.get("content_url") or "",
                                 "tipo": s.get("tipo"),
                                 "mode": s.get("mode")}
            sched_manual.pop(key, None)
        else:
            # fine fascia: torna al contenuto base (intatto, non
            # sovrascritto dal palinsesto)
            sched_active.pop(key, None)
        _sched_emit_target(key)
        applied = True
    if applied:
        broadcast_stato()


def scheduler_loop():
    """Worker in background: valuta il palinsesto ogni 30s. Le eccezioni
    non devono mai uccidere il worker."""
    socketio.sleep(5)
    while True:
        try:
            sched_tick()
        except Exception:
            pass
        # dorme fino al secondo ~1 del minuto successivo: le fasce
        # iniziano/finiscono entro ~1s dal cambio minuto
        _n = datetime.now()
        socketio.sleep(max(2.0, 61.0 - _n.second - _n.microsecond / 1e6))


# ---------------------------------------------------------------- auth

def check_auth():
    a = request.authorization
    return bool(a) and a.username == ADMIN_USER and a.password == ADMIN_PASSWORD


def requires_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not check_auth():
            return Response("Autenticazione richiesta", 401,
                            {"WWW-Authenticate": 'Basic realm="Signage Admin"'})
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------- pagine

@app.route("/")
def index():
    return ('<meta http-equiv="refresh" content="0; url=/admin">'
            '<a href="/admin">Admin</a>')


@app.route("/health")
def health():
    """Healthcheck pubblico per Coolify/load balancer (senza Basic Auth)."""
    return {"status": "ok"}, 200


def _player_response(gruppo, content):
    """Player HTML sempre fresco: no-store impedisce cache stale sulla TV."""
    resp = make_response(render_template("player.html", gruppo=gruppo,
                                         content=content))
    resp.headers["Cache-Control"] = "no-store, must-revalidate"
    return resp


@app.route("/tv")
def player_device():
    """Player unificato: il gruppo viene risolto dal device_id via WS."""
    return _player_response(None, idle_payload())


@app.route("/tv/<nome_gruppo>")
def player(nome_gruppo):
    g = get_or_create_gruppo(nome_gruppo)
    return _player_response(nome_gruppo, content_payload(g))


@app.route("/admin")
@requires_auth
def admin():
    resp = make_response(render_template("admin.html"))
    resp.headers["Cache-Control"] = "no-store, must-revalidate"
    return resp


@app.route("/media/<path:filename>")
def media(filename):
    resp = send_from_directory(MEDIA_DIR, filename)
    resp.headers["Cache-Control"] = "no-cache, must-revalidate"
    return resp


SKIP_HEADERS = {"x-frame-options", "content-security-policy",
                "content-security-policy-report-only", "frame-options",
                "content-length", "content-encoding", "transfer-encoding",
                "connection", "content-type"}

# Iniettato dopo <head> nelle pagine HTML proxate: le chiamate interne
# fetch()/XMLHttpRequest verso l'origine target ripassano da /proxy
# (stessa origine del player) -> niente blocchi CORS del browser.
PROXY_INTERCEPT = """<script>
(function () {
  var ORIGIN = %s;
  // assoluto sull'origine del server signage: con il <base> remoto un
  // path relativo "/proxy?u=" verrebbe risolto verso l'origine target
  var PROX = location.origin + "/proxy?u=";
  function prox(u) {
    if (typeof u !== "string" || !u) return u;
    var abs;
    try { abs = new URL(u, document.baseURI).href; }
    catch (e) { return u; }
    return abs.indexOf(ORIGIN) === 0 ? PROX + encodeURIComponent(abs) : u;
  }
  var of = window.fetch;
  window.fetch = function (input, init) {
    if (typeof input === "string") { input = prox(input); }
    else if (input instanceof Request) {
      input = new Request(prox(input.url), input);
    }
    return of.call(this, input, init);
  };
  var oo = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (method, url) {
    var rest = Array.prototype.slice.call(arguments, 2);
    return oo.apply(this, [method, prox(url)].concat(rest));
  };
  // risorse same-origin iniettate via JS (innerHTML, setAttribute,
  // img.src = ...): il rewrite statico lato server copre solo l'HTML
  // iniziale. Le richieste ripassano da /proxy locale.
  var ATTRS = ["src", "data-src", "poster"];
  function fixEl(el) {
    if (!el || !el.getAttribute) return;
    for (var i = 0; i < ATTRS.length; i++) {
      var a = ATTRS[i];
      var v = el.getAttribute(a);
      if (v && v.indexOf(PROX) !== 0) {
        var nv = prox(v);
        if (nv !== v) el.setAttribute(a, nv);
      }
    }
  }
  function fixTree(root) {
    if (!root) return;
    fixEl(root);
    if (root.querySelectorAll) {
      var nodes = root.querySelectorAll(
        "img, source, video, track, audio");
      for (var i = 0; i < nodes.length; i++) fixEl(nodes[i]);
    }
  }
  new MutationObserver(function (ms) {
    ms.forEach(function (m) {
      if (m.type === "attributes") fixEl(m.target);
      if (m.addedNodes) {
        m.addedNodes.forEach(function (n) {
          if (n.nodeType === 1) fixTree(n);
        });
      }
    });
  }).observe(document.documentElement || document,
             { childList: true, subtree: true, attributes: true,
               attributeFilter: ATTRS });
  document.addEventListener("DOMContentLoaded",
                            function () { fixTree(document); });
})();
</script>"""


def _same_origin(a, b):
    pa, pb = urlparse(a), urlparse(b)
    return pa.scheme == pb.scheme and pa.netloc.lower() == pb.netloc.lower()


@app.route("/proxy", methods=["GET", "POST"])
def proxy_page():
    """Proxy lato server per URL esterni che bloccano gli iframe.
    Serve la pagina senza X-Frame-Options/CSP così il player può caricarla
    in iframe mantenendo il controllo WebSocket (reload, cambio contenuto).
    Accetta GET e POST (inoltrando body e cookie) perché le app interattive
    proxate riusano questa rotta per le loro chiamate interne; la whitelist
    copre l'URL configurato e ogni URL della sua stessa origine."""
    u = (request.args.get("u") or "").strip()
    if not u.lower().startswith(("http://", "https://")):
        abort(400)
    allowed = allowed_proxy_urls()
    if u not in allowed and not any(_same_origin(u, a) for a in allowed):
        abort(403)
    try:
        headers = {"User-Agent": PROXY_UA}
        # cookie e content-type inoltrati: login/session e POST delle
        # bacheche interattive proxate continuano a funzionare
        if request.headers.get("Cookie"):
            headers["Cookie"] = request.headers["Cookie"]
        if request.method == "POST":
            ct = request.headers.get("Content-Type")
            if ct:
                headers["Content-Type"] = ct
            r = ext_requests.post(u, data=request.get_data() or None,
                                  timeout=20, stream=True, headers=headers,
                                  allow_redirects=True)
        else:
            r = ext_requests.get(u, timeout=20, stream=True,
                                 headers=headers, allow_redirects=True)
    except ext_requests.RequestException:
        abort(502)
    ctype = r.headers.get("Content-Type", "")
    if "text/html" in ctype.lower():
        html = r.text
        r.close()
        p = urlparse(r.url)
        origin = f"{p.scheme}://{p.netloc}"
        # risorse same-origin (immagini, media, form) via /proxy: l'host
        # remoto puo' rifiutare le richieste dirette del browser (WAF,
        # referer); il fetch server-side usa PROXY_UA e il player riceve
        # la risorsa same-origin. Il path e' assoluto perche' il <base>
        # remoto risolverebbe un relativo verso l'origine target.
        _self = request.host_url.rstrip("/")

        def _rw_url(m):
            pre, q, url = m.groups()
            if _same_origin(url, r.url):
                return (f"{pre}{q}{_self}/proxy?u="
                        f"{quote(url, safe='')}{q}")
            return m.group(0)

        html = re.sub(
            r'(\s(?:src|poster|data-src|action)\s*=\s*)(["\'])'
            r'(https?://[^"\']+)\2', _rw_url, html, flags=re.I)
        # <base> per risolvere risorse/link relativi verso l'origine
        inject = (f'<base href="{r.url}">' +
                  PROXY_INTERCEPT % json.dumps(origin))
        m = re.search(r"<head[^>]*>", html, re.I)
        html = (html[:m.end()] + inject + html[m.end():]
                if m else inject + html)
        resp = Response(html, status=r.status_code, content_type=ctype)
    else:
        resp = Response(r.iter_content(64 * 1024), status=r.status_code,
                        content_type=ctype or "application/octet-stream")
    for k, v in r.headers.items():
        if k.lower() not in SKIP_HEADERS:
            resp.headers.setdefault(k, v)
    return resp


# ---------------------------------------------------------------- App & Widget
# Pagine pubbliche (servono i player TV, senza auth): ogni app e' una rotta
# HTML5 fullscreen che il player renderizza in iframe come contenuto 'url'.

@app.route("/apps/clock")
def app_clock():
    return render_template("apps/clock.html")


@app.route("/apps/weather")
def app_weather():
    return render_template("apps/weather.html")


@app.route("/apps/traffic")
def app_traffic():
    return render_template("apps/traffic.html")


@app.route("/apps/social/<widget_id>")
def app_social(widget_id):
    cfg = (state.get("app_configs") or {}).get(widget_id) or {}
    return render_template("apps/social.html",
                           nome=cfg.get("nome", widget_id),
                           html_snippet=cfg.get("html_snippet", ""))


@app.route("/apps/split")
def app_split():
    main_url = (request.args.get("main") or "").strip()
    side_url = (request.args.get("side") or "").strip()
    main_tipo = detect_tipo(main_url)
    if main_tipo == "youtube":
        # la watch page non e' frameabile: usa l'embed nocookie con autoplay
        embed = yt_embed_src(main_url)
        if embed:
            main_url, main_tipo = embed, "url"
    return render_template("apps/split.html", main_url=main_url,
                           main_tipo=main_tipo, side_url=side_url)


@app.route("/apps/news_tv")
def app_news_tv():
    """Diretta news fullscreen via hls.js per sorgenti .m3u8 (stream
    riprodotto dal browser del player -> usa l'IP italiano della sede,
    nessun proxy server-side); per Sky TG24 una sorgente YouTube usa
    l'embed youtube-nocookie. La sorgente di ogni canale sta in
    state['news_tv'] (config.json, editabile dall'admin); il parametro
    ?stream= permette un override puntuale sull'URL."""
    channel = (request.args.get("channel") or "skytg24").strip().lower()
    if channel not in NEWS_TV_DEFAULTS:
        channel = "skytg24"
    mute = request.args.get("mute", "1") != "0"
    badge = request.args.get("badge", "1") != "0"
    with state_lock:
        default_src = (state.get("news_tv") or {}).get(channel, "")
    stream = ((request.args.get("stream") or "").strip()
              or default_src or NEWS_TV_DEFAULTS[channel])
    is_hls = stream.lower().split("?")[0].endswith(".m3u8")
    yt_src = (news_tv_yt_src(stream, mute)
              if channel == "skytg24" and not is_hls else None)
    return render_template(
        "apps/news_tv.html", channel=channel,
        channel_name=("Sky TG24" if channel == "skytg24" else "Rai News 24"),
        mute=mute, badge=badge, stream=stream, yt_src=yt_src,
        hls_src=stream if is_hls else None)


# ---------------------------------------------------------------------------
# App: Lavagna Task & KPI Touch (Google Sheets / Webhook / Locale)
# ---------------------------------------------------------------------------
TASK_BOARDS_DIR = os.path.join(DATA_DIR, "task_boards")

DEFAULT_TASK_BOARD_DATA = {
    "tasks": [
        {
            "ID": "TSK-001",
            "Data": "",
            "Collaboratore": "Mario Rossi",
            "Titolo": "Controllo Qualità Lotto A",
            "Descrizione": "Verifica campionaria tolleranze e spessori",
            "Stato": "In corso",
            "Priorita": "Alta",
            "Note": "Avviato controllo primi 5 campioni",
        },
        {
            "ID": "TSK-002",
            "Data": "",
            "Collaboratore": "Luca Bianchi",
            "Titolo": "Taratura Strumenti Linea 2",
            "Descrizione": "Calibrazione calibri digitali e micrometri",
            "Stato": "Da fare",
            "Priorita": "Normale",
            "Note": "",
        },
        {
            "ID": "TSK-003",
            "Data": "",
            "Collaboratore": "Mario Rossi",
            "Titolo": "Verifica Certificati Materie Prime",
            "Descrizione": "Controllo conformità schede tecniche fornitore",
            "Stato": "Completato",
            "Priorita": "Normale",
            "Note": "Conforme",
        },
    ],
    "collaboratori": [
        {
            "Nome": "Mario Rossi",
            "PIN": "1234",
            "Ruolo": "Responsabile",
            "Email": "mario.rossi@example.com",
        },
        {
            "Nome": "Luca Bianchi",
            "PIN": "0000",
            "Ruolo": "Operatore",
            "Email": "luca.bianchi@example.com",
        },
    ],
    "kpis": [
        {
            "Collaboratore": "Mario Rossi",
            "Obiettivo": "Audit Conformità",
            "Target": "100%",
            "Attuale": "85%",
        },
        {
            "Collaboratore": "Luca Bianchi",
            "Obiettivo": "Lotti Verificati",
            "Target": "50",
            "Attuale": "42",
        },
    ],
}


def _parse_gviz_sheet(sheet_id, sheet_name):
    """Legge una scheda di un Google Spreadsheet via endpoint GViz pubblico/condiviso."""
    url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq"
           f"?tqx=out:json&sheet={quote(sheet_name)}")
    try:
        r = ext_requests.get(url, timeout=10)
        if r.status_code != 200:
            return []
        m = re.search(r"setResponse\((.*)\);", r.text, re.DOTALL)
        if not m:
            return []
        raw = json.loads(m.group(1))
        table = raw.get("table", {})
        cols = [str(c.get("label") or "").strip() for c in table.get("cols", [])]
        rows = []
        for r_entry in table.get("rows", []):
            cells = r_entry.get("c") or []
            row_obj = {}
            has_val = False
            for i, cell in enumerate(cells):
                val = ""
                if cell:
                    val = cell.get("f") if cell.get("f") is not None else cell.get("v")
                    val = "" if val is None else str(val).strip()
                if val:
                    has_val = True
                col_name = cols[i] if i < len(cols) and cols[i] else f"_col{i}"
                row_obj[col_name] = val
                row_obj[f"_col{i}"] = val
            if has_val:
                rows.append(row_obj)
        return rows
    except Exception as e:
        print(f"WARN: _parse_gviz_sheet({sheet_id}, {sheet_name}) errore: {e}")
        return []


# --- Google Sheets API v4 con Service Account (lettura/scrittura) ---
GOOGLE_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_sa_creds_cache = {}


def _sa_credentials(params):
    """Credenziali Service Account dal parametro 'service_account':
    accetta il JSON incollato oppure un percorso file (assoluto o
    relativo a DATA_DIR, es. 'sa.json')."""
    raw = (params.get("service_account") or "").strip()
    if not raw:
        return None
    if not _GOOGLE_AUTH_OK:
        print("WARN: google-auth non installato, Service Account non disponibile")
        return None
    creds = _sa_creds_cache.get(raw)
    if creds is None:
        try:
            if raw.startswith("{"):
                creds = _sa_module.Credentials.from_service_account_info(
                    json.loads(raw), scopes=GOOGLE_SHEETS_SCOPES)
            else:
                path = raw if os.path.isabs(raw) else os.path.join(DATA_DIR, raw)
                creds = _sa_module.Credentials.from_service_account_file(
                    path, scopes=GOOGLE_SHEETS_SCOPES)
        except Exception as e:
            print(f"WARN: credenziali Service Account non valide: {e}")
            return None
        _sa_creds_cache[raw] = creds
    return creds


def _sheets_api_request(method, sheet_id, path, creds, body=None,
                        params_qs=None):
    """Chiamata REST a Sheets API v4 con token OAuth2 del Service Account."""
    if not creds.valid:
        creds.refresh(_ga_request())
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}{path}"
    r = ext_requests.request(method, url, json=body, params=params_qs,
                             headers={"Authorization": f"Bearer {creds.token}"},
                             timeout=12)
    if r.status_code != 200:
        print(f"WARN: Sheets API {method} {path} -> {r.status_code}: "
              f"{r.text[:300]}")
        return None
    return r.json()


def _values_to_objects(values):
    """Converte la matrice 'values' dell'API in (header, [oggetti riga]).
    Ogni oggetto espone i valori per nome colonna, come _colN e il numero
    di riga foglio in _row (utile per la scrittura)."""
    if not values:
        return [], []
    header = [str(h or "").strip() for h in values[0]]
    rows = []
    for r_idx, row in enumerate(values[1:], start=2):
        obj = {"_row": r_idx}
        has_val = False
        for i in range(len(header)):
            val = ""
            if i < len(row) and row[i] is not None:
                val = str(row[i]).strip()
            if val:
                has_val = True
            h = header[i]
            if h:
                obj[h] = val
            obj[f"_col{i}"] = val
        if has_val:
            rows.append(obj)
    return header, rows


def _sheet_rows_via_api(sheet_id, sheet_name, creds):
    j = _sheets_api_request("GET", sheet_id,
                            f"/values/{quote(sheet_name, safe='')}", creds)
    if j is None:
        return []
    _, rows = _values_to_objects(j.get("values") or [])
    return rows


def _col_letter(idx):
    """Indice colonna 0-based -> notazione A1 (0->A, 9->J, ...)."""
    s = ""
    idx += 1
    while idx:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s


def _normalize_board_objects(raw_tasks, raw_collab, raw_kpi):
    """Normalizza le righe grezze (GViz o Sheets API) nel formato lavagna."""
    if not (raw_tasks or raw_collab or raw_kpi):
        return None
    tasks = []
    for t in raw_tasks:
        titolo = t.get("Titolo") or t.get("_col3") or ""
        if not str(titolo).strip():
            continue
        tasks.append({
            "ID": t.get("ID") or t.get("_col0") or "",
            "Data": t.get("Data") or t.get("_col1") or "",
            "Collaboratore": (t.get("Collaboratore")
                              or t.get("_col2") or ""),
            "Titolo": titolo,
            "Descrizione": (t.get("Descrizione")
                            or t.get("_col4") or ""),
            "Stato": t.get("Stato") or t.get("_col5") or "Da fare",
            "Priorita": (t.get("Priorità") or t.get("Priorita")
                         or t.get("_col6") or "Normale"),
            "Note": t.get("Note") or t.get("_col7") or "",
        })
    collabs = []
    for c in raw_collab:
        nome = c.get("Nome") or c.get("_col0") or ""
        if not str(nome).strip():
            continue
        collabs.append({
            "Nome": nome,
            "PIN": c.get("PIN") or c.get("_col1") or "0000",
            "Ruolo": (c.get("Ruolo")
                      or c.get("_col2") or "Collaboratore"),
            "Email": c.get("Email") or c.get("_col3") or "",
        })
    kpis = []
    for k in raw_kpi:
        obiettivo = k.get("Obiettivo") or k.get("_col1") or ""
        if not str(obiettivo).strip():
            continue
        kpis.append({
            "Collaboratore": (k.get("Collaboratore")
                              or k.get("_col0") or ""),
            "Obiettivo": obiettivo,
            "Target": k.get("Target") or k.get("_col2") or "100%",
            "Attuale": k.get("Attuale") or k.get("_col3") or "0%",
        })
    return {"tasks": tasks, "collaboratori": collabs, "kpis": kpis}


def write_task_to_sheet(sheet_id, task_id, nuovo_stato, note, creds):
    """Aggiorna Stato/Note della riga del task nel tab 'Task' via Sheets API."""
    j = _sheets_api_request("GET", sheet_id,
                            f"/values/{quote('Task', safe='')}", creds)
    if j is None:
        return False
    header, rows = _values_to_objects(j.get("values") or [])
    row_no = None
    for obj in rows:
        if str(obj.get("ID") or obj.get("_col0") or "").strip() == task_id:
            row_no = obj["_row"]
            break
    if not row_no:
        print(f"WARN: task {task_id} non trovato nel tab Task")
        return False
    hdr = {name: i for i, name in enumerate(header)}
    si = hdr.get("Stato", 5)
    ni = hdr.get("Note", 7)
    updates = []
    if nuovo_stato:
        updates.append({"range": f"Task!{_col_letter(si)}{row_no}",
                        "values": [[nuovo_stato]]})
    updates.append({"range": f"Task!{_col_letter(ni)}{row_no}",
                    "values": [[note]]})
    res = _sheets_api_request(
        "POST", sheet_id, "/values:batchUpdate", creds,
        body={"valueInputOption": "USER_ENTERED", "data": updates})
    return res is not None


def append_task_to_sheet(sheet_id, task, creds):
    """Accoda una riga nel tab 'Task' allineata all'header esistente."""
    j = _sheets_api_request("GET", sheet_id,
                            f"/values/{quote('Task!1:1', safe='')}", creds)
    header = []
    if j and j.get("values"):
        header = [str(h or "").strip() for h in j["values"][0]]
    if header:
        row = []
        for h in header:
            if h in ("Priorita", "Priorità"):
                row.append(task.get("Priorita", "") or "")
            else:
                row.append(task.get(h, "") or "")
    else:
        row = [task.get("ID", ""), task.get("Data", ""),
               task.get("Collaboratore", ""), task.get("Titolo", ""),
               task.get("Descrizione", ""), task.get("Stato", ""),
               task.get("Priorita", ""), task.get("Note", "")]
    res = _sheets_api_request(
        "POST", sheet_id,
        f"/values/{quote('Task', safe='')}:append", creds,
        body={"values": [row]},
        params_qs={"valueInputOption": "USER_ENTERED",
                   "insertDataOption": "INSERT_ROWS"})
    return res is not None


def mark_timeline_done(sheet_id, task_id, creds):
    """Replica segnaCompletatoSuTimeline: cerca l'ID in 'Schedulation'
    e scrive 'OK' nella colonna J della riga trovata."""
    j = _sheets_api_request("GET", sheet_id,
                            f"/values/{quote('Schedulation', safe='')}", creds)
    if j is None:
        return False
    values = j.get("values") or []
    for i, row in enumerate(values[1:], start=2):
        if any(str(c or "").strip() == task_id for c in row):
            res = _sheets_api_request(
                "POST", sheet_id, "/values:batchUpdate", creds,
                body={"valueInputOption": "USER_ENTERED",
                      "data": [{"range": f"Schedulation!J{i}",
                                "values": [["OK"]]}]})
            return res is not None
    print(f"WARN: task {task_id} non trovato in Schedulation")
    return False


def sync_task_board_from_remote(board_id, params):
    webhook = (params.get("webhook_url") or "").strip()
    sheet_id = (params.get("sheet_id") or "").strip()
    if webhook:
        try:
            r = ext_requests.get(webhook, timeout=10)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, dict):
                    d = j.get("data") if "data" in j else j
                    if isinstance(d, dict) and "tasks" in d:
                        return d
        except Exception as e:
            print(f"WARN: errore fetch webhook task board {board_id}: {e}")
    if sheet_id:
        # 1) Sheets API autenticata con Service Account (se configurato)
        creds = _sa_credentials(params)
        if creds:
            try:
                data = _normalize_board_objects(
                    _sheet_rows_via_api(sheet_id, "Task", creds),
                    _sheet_rows_via_api(sheet_id, "Collaboratori", creds),
                    _sheet_rows_via_api(sheet_id, "KPI", creds))
                if data:
                    return data
            except Exception as e:
                print(f"WARN: errore sync Sheets API {sheet_id}: {e}")
        # 2) Fallback GViz: funziona solo se il foglio e' condiviso col link
        try:
            data = _normalize_board_objects(
                _parse_gviz_sheet(sheet_id, "Task"),
                _parse_gviz_sheet(sheet_id, "Collaboratori"),
                _parse_gviz_sheet(sheet_id, "KPI"))
            if data:
                return data
        except Exception as e:
            print(f"WARN: errore sync google sheet {sheet_id}: {e}")
    return None


def get_task_board_data(board_id):
    os.makedirs(TASK_BOARDS_DIR, exist_ok=True)
    fp = os.path.join(TASK_BOARDS_DIR, f"{secure_filename(board_id)}.json")
    if os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"WARN: errore lettura task board {board_id}: {e}")
    with state_lock:
        cfg = (state.get("app_configs") or {}).get(board_id) or {}
    params = cfg.get("params") or {}
    data = sync_task_board_from_remote(board_id, params)
    if data:
        save_task_board_data(board_id, data)
        return data
    demo = json.loads(json.dumps(DEFAULT_TASK_BOARD_DATA))
    oggi_str = datetime.now(LOCAL_TZ).strftime("%d/%m/%Y")
    for t in demo["tasks"]:
        t["Data"] = oggi_str
    save_task_board_data(board_id, demo)
    return demo


def save_task_board_data(board_id, data):
    os.makedirs(TASK_BOARDS_DIR, exist_ok=True)
    fp = os.path.join(TASK_BOARDS_DIR, f"{secure_filename(board_id)}.json")
    tmp = fp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, fp)


@app.route("/apps/task_board/<board_id>")
@app.route("/apps/task_board")
def app_task_board(board_id="default"):
    with state_lock:
        cfg = (state.get("app_configs") or {}).get(board_id) or {}
    params = cfg.get("params") or {}
    titolo = (params.get("title") or cfg.get("nome")
              or "Piano Operativo Controllo Qualità")
    return render_template("apps/task_board.html", board_id=board_id,
                           titolo=titolo, nome=cfg.get("nome", board_id))


@app.route("/api/task_board/<board_id>/dati")
def api_task_board_dati(board_id):
    data = get_task_board_data(board_id)
    return jsonify({"ok": True, "data": data})


@app.route("/api/task_board/<board_id>/aggiorna", methods=["POST"])
def api_task_board_aggiorna(board_id):
    req_data = request.get_json(force=True) or {}
    task_id = str(req_data.get("id") or "").strip()
    nuovo_stato = str(req_data.get("nuovoStato") or "").strip()
    note = str(req_data.get("note") or "").strip()
    if not task_id:
        return jsonify({"ok": False, "error": "ID task mancante"}), 400

    board_data = get_task_board_data(board_id)
    found = False
    target_task = None
    for t in board_data.get("tasks", []):
        if str(t.get("ID") or "").strip() == task_id:
            t["Stato"] = nuovo_stato
            t["Note"] = note
            t["UltimoAggiornamento"] = datetime.now(LOCAL_TZ).isoformat()
            found = True
            target_task = t
            break

    if not found:
        return jsonify({"ok": False, "error": "Task non trovato"}), 404

    save_task_board_data(board_id, board_data)

    with state_lock:
        cfg = (state.get("app_configs") or {}).get(board_id) or {}
    params = cfg.get("params") or {}
    webhook = (params.get("webhook_url") or "").strip()
    if webhook:
        try:
            ext_requests.post(webhook, json={
                "action": "aggiornaTask",
                "id": task_id,
                "nuovoStato": nuovo_stato,
                "nuoveNote": note,
                "task": target_task,
                "timeline_sheet_id": params.get("timeline_sheet_id", "")
            }, timeout=8)
        except Exception as e:
            print(f"WARN: webhook aggiornaTask fallito: {e}")

    # Scrittura diretta su Google Sheet via Service Account
    creds = _sa_credentials(params)
    sheet_id = (params.get("sheet_id") or "").strip()
    if creds and sheet_id:
        try:
            write_task_to_sheet(sheet_id, task_id, nuovo_stato, note, creds)
            tl_id = (params.get("timeline_sheet_id") or "").strip()
            if tl_id and nuovo_stato.strip().lower() == "completato":
                mark_timeline_done(tl_id, task_id, creds)
        except Exception as e:
            print(f"WARN: scrittura Sheets API (aggiorna) fallita: {e}")

    return jsonify({"ok": True, "success": True})


@app.route("/api/task_board/<board_id>/crea", methods=["POST"])
def api_task_board_crea(board_id):
    req_data = request.get_json(force=True) or {}
    titolo = str(req_data.get("titolo") or "").strip()
    if not titolo:
        return jsonify({"ok": False, "error": "Titolo obbligatorio"}), 400

    board_data = get_task_board_data(board_id)
    tasks = board_data.setdefault("tasks", [])

    max_id = 0
    for t in tasks:
        m = re.search(r"(\d+)", str(t.get("ID") or ""))
        if m:
            val = int(m.group(1))
            if val > max_id:
                max_id = val

    nuovo_id = f"TSK-{max_id + 1:03d}"
    data_str = (str(req_data.get("data") or "").strip()
                or datetime.now(LOCAL_TZ).strftime("%d/%m/%Y"))

    new_task = {
        "ID": nuovo_id,
        "Data": data_str,
        "Collaboratore": str(req_data.get("collaboratore") or "").strip(),
        "Titolo": titolo,
        "Descrizione": str(req_data.get("descrizione") or "").strip(),
        "Stato": "Da fare",
        "Priorita": str(req_data.get("priorita") or "Normale").strip(),
        "Note": "",
        "Creato": datetime.now(LOCAL_TZ).isoformat()
    }
    tasks.insert(0, new_task)
    save_task_board_data(board_id, board_data)

    with state_lock:
        cfg = (state.get("app_configs") or {}).get(board_id) or {}
    params = cfg.get("params") or {}
    webhook = (params.get("webhook_url") or "").strip()
    if webhook:
        try:
            ext_requests.post(webhook, json={
                "action": "creaNuovoTask",
                "task": new_task
            }, timeout=8)
        except Exception as e:
            print(f"WARN: webhook creaNuovoTask fallito: {e}")

    # Scrittura diretta su Google Sheet via Service Account
    creds = _sa_credentials(params)
    sheet_id = (params.get("sheet_id") or "").strip()
    if creds and sheet_id:
        try:
            append_task_to_sheet(sheet_id, new_task, creds)
        except Exception as e:
            print(f"WARN: scrittura Sheets API (crea) fallita: {e}")

    return jsonify({"ok": True, "success": True, "task": new_task})


@app.route("/api/task_board/<board_id>/sync", methods=["POST"])
@requires_auth
def api_task_board_sync(board_id):
    with state_lock:
        cfg = (state.get("app_configs") or {}).get(board_id) or {}
    params = cfg.get("params") or {}
    remote_data = sync_task_board_from_remote(board_id, params)
    if remote_data:
        save_task_board_data(board_id, remote_data)
        return jsonify({"ok": True, "data": remote_data})
    return jsonify({"ok": False,
                    "error": "Impossibile sincronizzare dai server remoti"}), 502


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_TTL = 1800  # cache server-side 30 min: 1 chiamata esterna / 30 min


@app.route("/api/apps/weather_data")
def api_apps_weather():
    """Proxy pubblico verso Open-Meteo con cache 30 min e fallback stale.
    In caso di errore esterno restituisce l'ultimo dato valido (stale=true)
    invece di un 500, per resilienza sui player."""
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except (TypeError, ValueError):
        return jsonify({"ok": False,
                        "error": "Parametri lat/lon mancanti o non validi"}), 400
    key = (round(lat, 3), round(lon, 3))
    now = time.time()
    with state_lock:
        hit = weather_cache.get(key)
        if hit and now - hit["ts"] < WEATHER_TTL:
            return jsonify(hit["data"])
    try:
        r = ext_requests.get(
            OPEN_METEO_URL, timeout=10,
            params={"latitude": lat, "longitude": lon,
                    "current": "temperature_2m,relative_humidity_2m,"
                               "apparent_temperature,weather_code,"
                               "wind_speed_10m",
                    "daily": "weather_code,temperature_2m_max,"
                             "temperature_2m_min",
                    "timezone": "auto", "forecast_days": 4})
        r.raise_for_status()
        data = {"ok": True, "stale": False, "fetched_at": now,
                "meteo": r.json()}
    except (ext_requests.RequestException, ValueError):
        with state_lock:
            hit = weather_cache.get(key)
        if hit:  # ultimo dato valido anche se scaduto: niente pagina bianca
            return jsonify(dict(hit["data"], stale=True))
        return jsonify({"ok": False,
                        "error": "Servizio meteo non raggiungibile"}), 502
    with state_lock:
        weather_cache[key] = {"ts": now, "data": data}
    return jsonify(data)


@app.route("/api/apps/geocode")
@requires_auth
def api_apps_geocode():
    """Geocoding citta' -> lat/lon via Open-Meteo (per la UI admin)."""
    city = (request.args.get("city") or "").strip()
    if not city:
        return jsonify({"ok": False, "error": "Parametro city mancante"}), 400
    try:
        r = ext_requests.get(GEOCODE_URL, timeout=10,
                             params={"name": city, "count": 5,
                                     "language": "it", "format": "json"})
        r.raise_for_status()
        results = [{"name": x.get("name", ""),
                    "admin1": x.get("admin1", ""),
                    "country": x.get("country", ""),
                    "lat": x.get("latitude"), "lon": x.get("longitude")}
                   for x in (r.json().get("results") or [])]
    except (ext_requests.RequestException, ValueError):
        return jsonify({"ok": False,
                        "error": "Geocoding non raggiungibile"}), 502
    return jsonify({"ok": True, "results": results})


APP_TYPES = {"clock", "weather", "traffic", "social", "split", "news_tv", "task_board"}


@app.route("/api/apps/news_tv", methods=["POST"])
@requires_auth
def api_apps_news_tv():
    """Aggiorna le sorgenti delle dirette in state['news_tv'].
    Body: {skytg24?: <m3u8 URL|videoId|channelId|URL>, rainews24?: <m3u8 URL>}
    Le modifiche valgono per tutte le app/news_tv esistenti al prossimo
    caricamento della pagina (non serve ri-salvare ogni app)."""
    data = request.get_json(force=True)
    with state_lock:
        nt = state.setdefault("news_tv", dict(NEWS_TV_DEFAULTS))
        for k in NEWS_TV_DEFAULTS:
            v = str(data.get(k) or "").strip()
            if v:
                nt[k] = v[:500]
        nt = dict(nt)
    save_config()
    broadcast_stato()
    return jsonify({"ok": True, "news_tv": nt})


@app.route("/api/apps", methods=["POST"])
@requires_auth
def api_apps_save():
    """Salva una app configurata in state['app_configs'].
    Body: {id?, nome, tipo, url?, params?, html_snippet?}
    L'html_snippet (embed social) e' accettato SOLO qui, dietro Basic Auth:
    viene renderizzato grezzo dalla rotta pubblica /apps/social/<id>."""
    data = request.get_json(force=True)
    nome = str(data.get("nome") or "").strip()[:80]
    tipo = str(data.get("tipo") or "").strip()
    if tipo not in APP_TYPES:
        return jsonify({"ok": False, "error": "Tipo app non valido"}), 400
    app_id = str(data.get("id") or "").strip()
    if app_id and not re.fullmatch(r"[\w-]{1,80}", app_id):
        return jsonify({"ok": False, "error": "ID app non valido"}), 400
    with state_lock:
        cfgs = state.setdefault("app_configs", {})
        if not app_id:
            slug = re.sub(r"[^a-z0-9]+", "_", nome.lower()).strip("_") or tipo
            app_id = f"{tipo}_{slug}"[:60]
            base, n = app_id, 2
            while app_id in cfgs:
                app_id = f"{base}_{n}"
                n += 1
        params = data.get("params")
        entry = {"nome": nome or app_id, "tipo": tipo,
                 "url": str(data.get("url") or "")[:2000],
                 "created": (cfgs.get(app_id) or {}).get("created") or now_iso(),
                 "updated": now_iso()}
        if isinstance(params, dict):
            entry["params"] = {str(k)[:40]: str(v)[:500]
                               for k, v in params.items()
                               if isinstance(v, (str, int, float, bool))}
        if tipo == "social":
            entry["html_snippet"] = str(data.get("html_snippet") or "")[:20000]
            entry["url"] = f"/apps/social/{app_id}"
        elif tipo == "task_board":
            entry["url"] = f"/apps/task_board/{app_id}"
        cfgs[app_id] = entry
    save_config()
    broadcast_stato()
    return jsonify({"ok": True, "id": app_id, "url": entry["url"]})


@app.route("/api/apps/<app_id>", methods=["DELETE"])
@requires_auth
def api_apps_delete(app_id):
    with state_lock:
        if state.get("app_configs", {}).pop(app_id, None) is None:
            return jsonify({"ok": False,
                            "error": "App inesistente"}), 404
    save_config()
    broadcast_stato()
    return jsonify({"ok": True})


# ---------------------------------------------------------------- API

@app.route("/api/stato")
@requires_auth
def api_stato():
    return jsonify(build_stato())


def _target_gruppi(data):
    """Restituisce la lista dei gruppi destinatari dalla richiesta."""
    sel = data.get("gruppi", "all")
    with state_lock:
        all_g = list(state["gruppi"].keys())
    if sel == "all" or sel == ["all"]:
        # 'default' e' la sala di parcheggio dei device appena registrati:
        # il broadcast globale non deve mai sovrascriverla
        return [g for g in all_g if g != DEFAULT_GROUP]
    if isinstance(sel, str):
        sel = [sel]
    return [g for g in sel if g in all_g]


@app.route("/api/contenuto", methods=["POST"])
@requires_auth
def api_contenuto():
    data = request.get_json(force=True)
    url = (data.get("url") or "").strip()
    mode = data.get("mode", "auto")
    targets = _target_gruppi(data)
    if not targets:
        return jsonify({"ok": False, "error": "Nessun gruppo valido"}), 400
    if mode not in VALID_MODES:
        mode = "auto"
    tipo = detect_tipo(url)
    if tipo == "url" and mode == "auto":
        mode = "iframe" if check_frameable(url) else "proxy"
    elif tipo != "url":
        mode = "iframe"  # irrilevante per media, normalizzato
    with state_lock:
        for nome in targets:
            g = state["gruppi"][nome]
            sched_manual["g:" + nome] = True   # 'Metti in onda' manuale
            sched_active.pop("g:" + nome, None)  # il manuale sostituisce l'overlay
            g["playlist"] = []
            g["contenuto_attivo"] = url
            g["tipo"] = tipo
            g["mode"] = mode
            g["ultimo_aggiornamento"] = now_iso()
    for nome in targets:
        stop_playlist(nome)
        emit_content(nome)
    save_config()
    broadcast_stato()
    return jsonify({"ok": True, "targets": targets})


@app.route("/api/upload", methods=["POST"])
@requires_auth
def api_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "Nessun file"}), 400
    filename = secure_filename(f.filename)
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({"ok": False,
                        "error": f"Estensione .{ext} non consentita"}), 400
    os.makedirs(MEDIA_DIR, exist_ok=True)
    f.save(os.path.join(MEDIA_DIR, filename))
    broadcast_stato()
    return jsonify({"ok": True, "path": f"/media/{filename}",
                    "tipo": detect_tipo(filename)})


@app.route("/api/media/delete", methods=["POST"])
@requires_auth
def api_media_delete():
    data = request.get_json(force=True)
    name = secure_filename(data.get("file", ""))
    path = os.path.join(MEDIA_DIR, name)
    if name and os.path.isfile(path):
        os.remove(path)
        broadcast_stato()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "File non trovato"}), 404


@app.route("/api/reload", methods=["POST"])
@requires_auth
def api_reload():
    """Body: {"gruppi": "all"|[..]} oppure {"dispositivi": [dev_id, ...]}."""
    data = request.get_json(force=True)
    devs = data.get("dispositivi")
    if devs:
        if isinstance(devs, str):
            devs = [devs]
        for d in devs:
            socketio.emit("force_reload", {}, to=f"dev:{d}")
        return jsonify({"ok": True, "targets": devs})
    targets = _target_gruppi(data)
    for nome in targets:
        socketio.emit("force_reload", {}, to=nome)
    return jsonify({"ok": True, "targets": targets})


@app.route("/api/dispositivo/<dev_id>", methods=["POST"])
@app.route("/api/dispositivi/<dev_id>", methods=["POST"])
@requires_auth
def api_dispositivo(dev_id):
    """Body: {"nome": "...", "gruppo": "..."} — rename e/o riassegnazione.
    'gruppo' inesistente viene creato; 'non_assegnato' sgancia il device.
    La riassegnazione azzera l'eventuale contenuto dedicato."""
    data = request.get_json(force=True)
    with state_lock:
        dev = state.setdefault("dispositivi", {}).get(dev_id)
        if not dev:
            return jsonify({"ok": False, "error": "Dispositivo sconosciuto"}), 404
        old_group = dev.get("gruppo")
        if "nome" in data:
            dev["nome"] = str(data.get("nome") or "")[:60].strip()
        if "gruppo" in data:
            new_group = str(data.get("gruppo") or "").strip() or UNASSIGNED
            if new_group != UNASSIGNED:
                get_or_create_gruppo(new_group)
            dev["gruppo"] = new_group
            dev["contenuto"] = None  # torna al feed del gruppo
        new_group = dev["gruppo"]
    save_config()
    if "gruppo" in data and new_group != old_group:
        move_device_room(dev_id, old_group, new_group)
    emit_to_device(dev_id)
    broadcast_stato()
    return jsonify({"ok": True, "device": dev})


@app.route("/api/dispositivi/<dev_id>/identify", methods=["POST"])
@app.route("/api/dispositivo/<dev_id>/identify", methods=["POST"])
@requires_auth
def api_dispositivo_identify(dev_id):
    """Mostra per 8s un overlay identificativo sul device (id+nome+gruppo)."""
    with state_lock:
        dev = state["dispositivi"].get(dev_id)
        if not dev:
            return jsonify({"ok": False, "error": "Dispositivo sconosciuto"}), 404
        payload = {"device_id": dev_id, "nome": dev.get("nome") or dev_id,
                   "gruppo": dev.get("gruppo", UNASSIGNED)}
    socketio.emit("identify", payload, to=f"dev:{dev_id}")
    return jsonify({"ok": True})


@app.route("/api/dispositivi/<dev_id>/contenuto", methods=["POST"])
@app.route("/api/dispositivo/<dev_id>/contenuto", methods=["POST"])
@requires_auth
def api_dispositivo_contenuto(dev_id):
    """Contenuto dedicato al singolo device (Trasmetti solo qui).
    {"url": "..."} => override; {"url": null} => torna al feed del gruppo;
    {"type": "none"} => override idle: schermata di attesa/pairing con
    il codice del device, isolato dal feed del gruppo.
    Con override attivo il device esce dalla room del gruppo."""
    data = request.get_json(force=True)
    url = (data.get("url") or "").strip() or None
    want_idle = str(data.get("type") or data.get("tipo")
                    or "").strip().lower() in ("none", "idle")
    mode = data.get("mode", "auto")
    with state_lock:
        dev = state["dispositivi"].get(dev_id)
        if not dev:
            return jsonify({"ok": False, "error": "Dispositivo sconosciuto"}), 404
        gname = dev.get("gruppo", UNASSIGNED)
        if want_idle:
            dev["contenuto"] = {"url": "", "tipo": "idle", "mode": "iframe",
                                "ultimo_aggiornamento": now_iso()}
        elif url:
            if mode not in VALID_MODES:
                mode = "auto"
            tipo = detect_tipo(url)
            if tipo == "url" and mode == "auto":
                mode = "iframe" if check_frameable(url) else "proxy"
            elif tipo != "url":
                mode = "iframe"
            dev["contenuto"] = {"url": url, "tipo": tipo, "mode": mode,
                                "ultimo_aggiornamento": now_iso()}
        else:
            dev["contenuto"] = None
        if url or want_idle:
            sched_manual["d:" + dev_id] = True   # override dedicato manuale
        else:
            # override rimosso: il device torna al feed di gruppo e il
            # palinsesto puo' riprendere il controllo
            sched_manual.pop("d:" + dev_id, None)
        sched_active.pop("d:" + dev_id, None)
    save_config()
    if url or want_idle:
        move_device_room(dev_id, gname, None)   # isola dal feed di gruppo
    else:
        move_device_room(dev_id, None, gname)   # rientra nella room di gruppo
    emit_to_device(dev_id)
    broadcast_stato()
    return jsonify({"ok": True, "dedicato": bool(url or want_idle)})


@app.route("/api/dispositivi/<dev_id>", methods=["DELETE"])
@app.route("/api/dispositivo/<dev_id>", methods=["DELETE"])
@requires_auth
def api_dispositivo_delete(dev_id):
    """Elimina il record dispositivo (tipicamente per TV offline).
    Se il device torna online si re-registrerà come nuovo nel gruppo
    'default'; eventuali sid residui vengono sganciati dalle room."""
    with state_lock:
        dev = state.get("dispositivi", {}).pop(dev_id, None)
        sched_active.pop("d:" + dev_id, None)
        sched_manual.pop("d:" + dev_id, None)
        if dev is None:
            return jsonify({"ok": False,
                            "error": "Dispositivo sconosciuto"}), 404
        sids = device_sids.pop(dev_id, set())
        for sid in sids:
            g = sid_group.pop(sid, None)
            if g and g in clients:
                clients[g].discard(sid)
                if not clients[g]:
                    del clients[g]
            sid_device.pop(sid, None)
            try:
                leave_room(f"dev:{dev_id}", sid=sid, namespace='/')
            except Exception:
                pass
        save_config()
    broadcast_stato()
    return jsonify({"ok": True})


@app.route("/api/gruppi", methods=["POST"])
@requires_auth
def api_gruppi_create():
    """Body: {"nome": "..."} — creazione esplicita di un gruppo vuoto."""
    data = request.get_json(force=True)
    nome = str(data.get("nome") or "").strip()
    if not nome or nome == UNASSIGNED or "/" in nome:
        return jsonify({"ok": False, "error": "Nome gruppo non valido"}), 400
    esistente = nome in state["gruppi"]
    get_or_create_gruppo(nome)
    broadcast_stato()
    return jsonify({"ok": True, "esistente": esistente})


@app.route("/api/playlist", methods=["POST"])
@requires_auth
def api_playlist():
    """Body: {"gruppo": "sala", "playlist": [{"url": "...", "durata_sec": 15}]}
    Playlist vuota/nulla => stop."""
    data = request.get_json(force=True)
    nome = data.get("gruppo")
    items = data.get("playlist") or []
    with state_lock:
        g = state["gruppi"].get(nome)
        if not g:
            return jsonify({"ok": False, "error": "Gruppo inesistente"}), 404
        clean = []
        for i in items:
            if not i.get("url"):
                continue
            try:
                # 0 consentito: su YouTube significa "slide a fine video"
                dur = max(0, int(i.get("durata_sec", 15)))
            except (TypeError, ValueError):
                dur = 15
            clean.append({"url": str(i["url"]), "durata_sec": dur})
        g["playlist"] = clean
        g["ultimo_aggiornamento"] = now_iso()
        sched_manual["g:" + nome] = True      # playlist impostata a mano
        sched_active.pop("g:" + nome, None)   # la playlist sostituisce l'overlay
    if clean:
        start_playlist(nome)
    else:
        stop_playlist(nome)
        emit_content(nome)
    save_config()
    broadcast_stato()
    return jsonify({"ok": True, "playlist": clean})


@app.route("/api/gruppo/<nome>", methods=["DELETE"])
@requires_auth
def api_gruppo_delete(nome):
    with state_lock:
        if nome not in state["gruppi"]:
            return jsonify({"ok": False, "error": "Gruppo inesistente"}), 404
        del state["gruppi"][nome]
        sched_active.pop("g:" + nome, None)
        sched_manual.pop("g:" + nome, None)
        # i device assegnati al gruppo eliminato passano al gruppo 'default'
        orphan = [d for d, dev in state["dispositivi"].items()
                  if dev.get("gruppo") == nome]
        if orphan:
            get_or_create_gruppo(DEFAULT_GROUP)
        for d in orphan:
            state["dispositivi"][d]["gruppo"] = DEFAULT_GROUP
    stop_playlist(nome)
    socketio.emit("force_reload", {"redirect": "/"}, to=nome)
    for d in orphan:
        dev = state["dispositivi"][d]
        if not (dev.get("contenuto") or {}).get("url"):
            move_device_room(d, nome, DEFAULT_GROUP)
        else:
            move_device_room(d, nome, None)
        emit_to_device(d)
    save_config()
    broadcast_stato()
    return jsonify({"ok": True, "dispositivi_rilasciati": orphan})


# ---------------------------------------------------------------- Palinsesto API

HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


@app.route("/api/schedules", methods=["POST"])
@requires_auth
def api_schedules_save():
    """Crea/aggiorna una fascia del palinsesto.
    {id?, name?, target: 'all'|'group:<g>'|'device:<d>'|'<g>'|'<d>',
     content_url, mode?, days:[1..7] (1=Lun), start_time, end_time, enabled?}
    Con solo {id, enabled} aggiorna lo stato attivo senza validare il resto
    (toggle dalla tabella)."""
    data = request.get_json(force=True)
    sid = str(data.get("id") or "").strip()
    with state_lock:
        scheds = state.setdefault("schedules", [])
        ex = next((x for x in scheds if x.get("id") == sid), None)
        if ex and not (data.get("content_url") or "").strip():
            if "enabled" in data:
                ex["enabled"] = bool(data["enabled"])
            if data.get("name"):
                ex["name"] = str(data["name"]).strip()[:80]
            rec = dict(ex)
        else:
            target = str(data.get("target") or "").strip()
            url = str(data.get("content_url") or "").strip()
            try:
                days = sorted({int(d) for d in data.get("days") or []
                               if 1 <= int(d) <= 7})
            except (TypeError, ValueError):
                days = []
            st = str(data.get("start_time") or "").strip()
            et = str(data.get("end_time") or "").strip()
            mode = data.get("mode", "iframe")
            if mode not in VALID_MODES:
                mode = "iframe"
            if target != "all" and not target.startswith(("group:",
                                                          "device:")):
                target = ("device:" if target in state["dispositivi"]
                          else "group:") + target
            ok_t = (target == "all"
                    or (target.startswith("group:")
                        and target[6:] in state["gruppi"])
                    or (target.startswith("device:")
                        and target[7:] in state["dispositivi"]))
            if (not target or not ok_t or not url or not days
                    or not HHMM.match(st) or not HHMM.match(et) or st >= et):
                return jsonify({"ok": False,
                                "error": "Dati fascia non validi "
                                         "(target, giorni, orari)"}), 400
            rec = {"id": sid or f"sched_{int(time.time() * 1000)}",
                   "name": str(data.get("name") or "").strip()[:80] or url,
                   "target": target, "content_url": url,
                   "tipo": detect_tipo(url), "mode": mode, "days": days,
                   "start_time": st, "end_time": et,
                   "enabled": bool(data.get("enabled", True))}
            if ex:
                ex.update(rec)
                rec = dict(ex)
                # fascia aggiornata: invalida lo stato runtime cosi' il
                # contenuto editato viene riapplicato e l'override manuale
                # scade (l'edit e' un 'cambio fascia')
                for k in [k for k, v in sched_active.items()
                          if v.get("sched_id") == rec["id"]]:
                    sched_active.pop(k, None)
                for k in [k for k, v in sched_manual.items()
                          if v == rec["id"]]:
                    sched_manual.pop(k, None)
            else:
                scheds.append(rec)
    save_config()
    sched_tick()                 # applicazione immediata se la fascia e' attiva
    broadcast_stato()
    return jsonify({"ok": True, "schedule": rec})


@app.route("/api/schedules/<sid>", methods=["DELETE"])
@requires_auth
def api_schedules_delete(sid):
    with state_lock:
        scheds = state.get("schedules") or []
        if not any(s.get("id") == sid for s in scheds):
            return jsonify({"ok": False, "error": "Fascia inesistente"}), 404
        state["schedules"] = [s for s in scheds if s.get("id") != sid]
    save_config()
    sched_tick()                 # la fascia appena rimossa puo' liberare target
    broadcast_stato()
    return jsonify({"ok": True})


@app.route("/api/schedules/resume", methods=["POST"])
@requires_auth
def api_schedules_resume():
    """Annulla l'override manuale e rivaluta subito il palinsesto.
    {target?: 'g:<gruppo>'|'d:<dev_id>'} — senza target riprende tutto."""
    data = request.get_json(force=True)
    t = str(data.get("target") or "").strip()
    if t:
        sched_active.pop(t, None)
        sched_manual.pop(t, None)
    else:
        sched_active.clear()
        sched_manual.clear()
    sched_tick()
    broadcast_stato()
    return jsonify({"ok": True})


@app.route("/api/gruppo/<nome>/default", methods=["POST"])
@requires_auth
def api_gruppo_default(nome):
    """Imposta/annulla il contenuto predefinito del gruppo (fallback che il
    palinsesto ripristina a fine fascia).
    {use_current: true} copia il contenuto in onda; {url, mode?} lo imposta;
    {url: null} lo rimuove."""
    data = request.get_json(force=True)
    with state_lock:
        g = state["gruppi"].get(nome)
        if not g:
            return jsonify({"ok": False, "error": "Gruppo inesistente"}), 404
        if data.get("use_current"):
            url = g.get("contenuto_attivo") or ""
            if not url:
                return jsonify({"ok": False,
                                "error": "Nessun contenuto attivo da copiare"}), 400
            d = {"url": url, "tipo": g.get("tipo") or detect_tipo(url),
                 "mode": g.get("mode") or "iframe"}
        else:
            url = str(data.get("url") or "").strip()
            if url:
                tipo = detect_tipo(url)
                mode = data.get("mode", "auto")
                if mode not in VALID_MODES:
                    mode = "auto"
                if tipo == "url" and mode == "auto":
                    mode = "iframe" if check_frameable(url) else "proxy"
                elif tipo != "url":
                    mode = "iframe"
                d = {"url": url, "tipo": tipo, "mode": mode}
            else:
                d = None
        if d:
            g["default_content"] = d
        else:
            g.pop("default_content", None)
    save_config()
    emit_content(nome)              # aggiorna subito il base se non c'e' fascia
    broadcast_stato()
    return jsonify({"ok": True, "default_content": d})


# ---------------------------------------------------------------- WebSocket

@socketio.on("connect")
def on_connect():
    socketio.emit("stato", build_stato(), to=request.sid)


@socketio.on("join_room")
def on_join(data):
    data = data or {}
    nome = (data.get("gruppo") or "").strip() or None
    dev_id = (data.get("device_id") or "").strip() or None
    sid = request.sid

    if dev_id:
        # flusso device: gruppo risolto dalla mappatura (URL = solo default iniziale)
        dev = register_device(dev_id, nome,
                              user_agent=data.get("user_agent"))
        join_room(f"dev:{dev_id}")
        device_sids.setdefault(dev_id, set()).add(sid)
        sid_device[sid] = dev_id
        group = dev["gruppo"]
        # niente room di gruppo se il device ha contenuto dedicato o una
        # fascia palinsesto attiva (sched_active lo isola dal feed)
        if (group and group != UNASSIGNED and group in state["gruppi"]
                and not (dev.get("contenuto") or {}).get("url")
                and not sched_active.get("d:" + dev_id)):
            join_room(group)
            sid_group[sid] = group
            clients.setdefault(group, set()).add(sid)
        emit_to_device(dev_id)
    elif nome:
        # flusso legacy: player senza device_id punta direttamente il gruppo
        get_or_create_gruppo(nome)
        join_room(nome)
        sid_group[sid] = nome
        clients.setdefault(nome, set()).add(sid)
        emit_content(nome)
    else:
        return
    broadcast_stato()


@socketio.on("playlist_next")
def on_playlist_next(data):
    """Il player segnala la fine della slide multimediale (es. video YouTube
    ENDED via IFrame API): la playlist del gruppo avanza subito invece di
    attendere durata_sec. Guardie: il segnale vale solo se l'URL e' ancora
    quello in onda (anti doppi/stale) e dopo un dwell minimo di 2s."""
    data = data or {}
    sid = request.sid
    nome = sid_group.get(sid)
    if not nome:
        dev_id = sid_device.get(sid)
        if dev_id:
            with state_lock:
                nome = (state["dispositivi"].get(dev_id) or {}).get("gruppo")
    w = playlist_workers.get(nome or "")
    if not w:
        return
    url = data.get("url") or ""
    if url:
        with state_lock:
            g = state["gruppi"].get(nome) or {}
            if g.get("contenuto_attivo") != url:
                return  # segnale stale: la slide e' gia' cambiata
    if time.time() - w.get("slide_ts", 0) < 2:
        return  # anti-flood: dwell minimo per slide
    w["advance"].set()


@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    dev_id = sid_device.pop(sid, None)
    if dev_id and dev_id in device_sids:
        device_sids[dev_id].discard(sid)
        if not device_sids[dev_id]:
            del device_sids[dev_id]
    nome = sid_group.pop(sid, None)
    if nome and nome in clients:
        clients[nome].discard(sid)
        if not clients[nome]:
            del clients[nome]
    broadcast_stato()


# ---------------------------------------------------------------- avvio

# Init all'import del modulo: eseguito sia con "python main.py" sia con
# "gunicorn main:app" (Coolify/Docker), dove __main__ non viene chiamato.
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)
load_config()
# riavvia le playlist attive dopo un reboot del server
for _nome, _g in list(state["gruppi"].items()):
    if _g.get("playlist"):
        start_playlist(_nome)

# worker del palinsesto: valuta le fasce attive ogni 30s
socketio.start_background_task(scheduler_loop)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print(f"Digital Signage server su http://0.0.0.0:{port}")
    print(f"  Admin : http://<IP-SERVER>:{port}/admin  (user: {ADMIN_USER})")
    print(f"  Player: http://<IP-SERVER>:{port}/tv/<nome_gruppo>")
    socketio.run(app, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)

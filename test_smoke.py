"""Smoke test end-to-end: simula un client TV via SocketIO e verifica
consegna comandi (content, force_reload) e proxy. Uso:
    python test_smoke.py   (con il server gia' avviato su :5000)
"""
import sys
import time
from datetime import datetime, timedelta

import requests
import socketio

BASE = "http://127.0.0.1:5000"
AUTH = ("admin", "admin")
GRUPPO = "testws"

received = []
sio = socketio.Client()


@sio.on("content")
def on_content(d):
    received.append(("content", d))


@sio.on("force_reload")
def on_reload(d):
    received.append(("force_reload", d))


@sio.on("stato")
def on_stato(d):
    received.append(("stato", d))


def main():
    sio.connect(BASE, wait_timeout=5)
    sio.emit("join_room", {"gruppo": GRUPPO})
    time.sleep(0.8)

    # 1. contenuto iniziale ricevuto al join
    assert any(e[0] == "content" for e in received), "nessun content al join"

    # 2. push contenuto -> evento 'content' nella room
    received.clear()
    r = requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                      json={"gruppi": [GRUPPO], "url": "/media/dummy.png",
                            "mode": "iframe"})
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    ev = [e for e in received if e[0] == "content"]
    assert ev and ev[-1][1]["url"].startswith("/media/dummy.png"), received
    assert "v=" in ev[-1][1]["url"], "cache-buster mancante"

    # 3. force_reload -> evento ricevuto dal client
    received.clear()
    r = requests.post(f"{BASE}/api/reload", auth=AUTH,
                      json={"gruppi": [GRUPPO]})
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    assert any(e[0] == "force_reload" for e in received), \
        "force_reload non ricevuto"

    # 4. proxy: 400 senza url, 403 per url non configurato,
    #    contenuto configurato e' in allowlist
    assert requests.get(f"{BASE}/proxy").status_code == 400
    assert requests.get(
        f"{BASE}/proxy", params={"u": "https://evil.example.com"}
    ).status_code == 403

    # 5. conteggio client nella room
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["clients"].get(GRUPPO) == 1, s["clients"]

    # cleanup gruppo di test
    requests.delete(f"{BASE}/api/gruppo/{GRUPPO}", auth=AUTH)
    sio.disconnect()

    # ================= device flow (v2.0) =================
    dev = socketio.Client()
    dev_events, identify_events = [], []
    dev.on("content", lambda d: dev_events.append(d))
    dev.on("identify", lambda d: identify_events.append(d))
    dev.connect(BASE, wait_timeout=5)
    dev.emit("join_room", {"device_id": "TV-TEST1",
                           "user_agent": "smoke-test/1.0"})
    time.sleep(0.8)

    # reset idempotente (run precedenti possono aver lasciato stato)
    requests.post(f"{BASE}/api/dispositivi/TV-TEST1", auth=AUTH,
                  json={"gruppo": "non_assegnato", "nome": ""})
    requests.post(f"{BASE}/api/dispositivi/TV-TEST1/contenuto", auth=AUTH,
                  json={"url": None})
    time.sleep(0.5)

    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    d = s["dispositivi"].get("TV-TEST1")
    assert d and d["online"] and d["gruppo"] == "non_assegnato", d
    assert dev_events and dev_events[-1]["tipo"] == "idle", dev_events

    # nuovo device sconosciuto -> auto-registrato nel gruppo 'default'
    newdev = socketio.Client()
    newdev.connect(BASE, wait_timeout=5)
    newdev.emit("join_room", {"device_id": "TV-NEW9"})
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    nd = s["dispositivi"].get("TV-NEW9")
    assert nd and nd["gruppo"] == "default" and nd["nome"] == "TV-NEW9", nd
    assert "default" in s["gruppi"], s["gruppi"].keys()
    newdev.disconnect()

    # creazione esplicita gruppo
    r = requests.post(f"{BASE}/api/gruppi", auth=AUTH,
                      json={"nome": "atrio"})
    assert r.json()["ok"], r.text

    # broadcast 'all': la sala di parcheggio 'default' resta esclusa
    r = requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                      json={"gruppi": "all", "url": "/media/all.png",
                            "mode": "iframe"})
    assert r.json().get("ok"), r.text
    assert "default" not in r.json()["targets"], r.json()
    assert "atrio" in r.json()["targets"], r.json()
    # l'invio ESPLICITO a 'default' continua a funzionare
    r = requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                      json={"gruppi": ["default"], "url": "",
                            "mode": "iframe"})
    assert r.json().get("ok") and r.json()["targets"] == ["default"], r.text

    # identify -> evento al solo device
    r = requests.post(f"{BASE}/api/dispositivi/TV-TEST1/identify", auth=AUTH)
    assert r.json()["ok"], r.text
    time.sleep(0.8)
    assert identify_events and identify_events[0]["device_id"] == "TV-TEST1", \
        identify_events

    # rename
    r = requests.post(f"{BASE}/api/dispositivi/TV-TEST1", auth=AUTH,
                      json={"nome": "TV Sala Prove"})
    assert r.json()["ok"], r.text

    # riassegnazione a gruppo esistente -> push contenuto al device
    dev_events.clear()
    r = requests.post(f"{BASE}/api/dispositivi/TV-TEST1", auth=AUTH,
                      json={"gruppo": "atrio"})
    assert r.json()["ok"], r.text
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["dispositivi"]["TV-TEST1"]["gruppo"] == "atrio"
    assert s["clients"].get("atrio") == 1, s["clients"]
    assert dev_events and dev_events[-1].get("gruppo") == "atrio", dev_events

    # contenuto al gruppo -> il device lo riceve via room di gruppo
    dev_events.clear()
    requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                  json={"gruppi": ["atrio"], "url": "/media/x.png",
                        "mode": "iframe"})
    time.sleep(0.8)
    assert any(e.get("url", "").startswith("/media/x.png")
               for e in dev_events), dev_events

    # contenuto dedicato -> override; i push di gruppo NON lo coprono
    dev_events.clear()
    r = requests.post(f"{BASE}/api/dispositivi/TV-TEST1/contenuto", auth=AUTH,
                      json={"url": "/media/ded.png", "mode": "iframe"})
    assert r.json()["ok"] and r.json()["dedicato"], r.text
    time.sleep(0.8)
    assert dev_events and dev_events[-1]["dedicato"], dev_events
    assert dev_events[-1]["url"].startswith("/media/ded.png")
    dev_events.clear()
    requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                  json={"gruppi": ["atrio"], "url": "/media/other.png",
                        "mode": "iframe"})
    time.sleep(0.8)
    assert not any(e.get("url", "").startswith("/media/other.png")
                   for e in dev_events), \
        "il push di gruppo non deve sovrascrivere il contenuto dedicato"

    # rimozione override -> rientra nella room e riceve il feed del gruppo
    dev_events.clear()
    requests.post(f"{BASE}/api/dispositivi/TV-TEST1/contenuto", auth=AUTH,
                  json={"url": None})
    time.sleep(0.8)
    assert dev_events and dev_events[-1]["url"].startswith("/media/other.png"), \
        dev_events

    # schermata pairing: override idle isola il device dal feed di gruppo
    dev_events.clear()
    r = requests.post(f"{BASE}/api/dispositivi/TV-TEST1/contenuto", auth=AUTH,
                      json={"type": "none"})
    assert r.json().get("ok") and r.json()["dedicato"], r.text
    time.sleep(0.8)
    assert dev_events and dev_events[-1]["tipo"] == "idle", dev_events
    # i push di gruppo non raggiungono il device in pairing
    dev_events.clear()
    requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                  json={"gruppi": ["atrio"], "url": "/media/z.png",
                        "mode": "iframe"})
    time.sleep(0.8)
    assert not any(e.get("url", "").startswith("/media/z.png")
                   for e in dev_events), dev_events
    # rimozione override -> rientra nella room e riceve il feed del gruppo
    dev_events.clear()
    requests.post(f"{BASE}/api/dispositivi/TV-TEST1/contenuto", auth=AUTH,
                  json={"url": None})
    time.sleep(0.8)
    assert dev_events and dev_events[-1]["url"].startswith("/media/z.png"), \
        dev_events

    # delete gruppo -> il device orfano passa a 'default'
    dev_events.clear()
    requests.delete(f"{BASE}/api/gruppo/atrio", auth=AUTH)
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["dispositivi"]["TV-TEST1"]["gruppo"] == "default"

    # delete device offline -> record rimosso, seconda delete = 404
    r = requests.delete(f"{BASE}/api/dispositivi/TV-NEW9", auth=AUTH)
    assert r.json()["ok"], r.text
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert "TV-NEW9" not in s["dispositivi"], s["dispositivi"]
    r = requests.delete(f"{BASE}/api/dispositivi/TV-NEW9", auth=AUTH)
    assert r.status_code == 404, r.text

    dev.disconnect()

    # ================= app & widget (v2.1) =================
    # pagine pubbliche delle app: tutte 200, nessuna auth richiesta
    for path in ("/apps/clock", "/apps/weather", "/apps/traffic",
                 "/apps/split", "/apps/social/widget_inesistente"):
        r = requests.get(BASE + path)
        assert r.status_code == 200, f"{path} -> {r.status_code}"

    # meteo: 400 senza lat/lon; con coordinate OK o 502 se offline (mai 500)
    r = requests.get(f"{BASE}/api/apps/weather_data")
    assert r.status_code == 400, r.status_code
    r = requests.get(f"{BASE}/api/apps/weather_data",
                     params={"lat": 45.5416, "lon": 10.2118})
    assert r.status_code in (200, 502), r.status_code
    if r.status_code == 200:
        # seconda chiamata servita da cache: stesso risultato
        r2 = requests.get(f"{BASE}/api/apps/weather_data",
                          params={"lat": 45.5416, "lon": 10.2118})
        assert r2.status_code == 200 and r2.json().get("ok"), r2.text

    # salvataggio app social (auth) -> render pubblico dello snippet
    r = requests.post(f"{BASE}/api/apps", auth=AUTH,
                      json={"nome": "Widget Test", "tipo": "social",
                            "html_snippet": "<div id='wt'>CIAO</div>"})
    assert r.json().get("ok"), r.text
    app_id = r.json()["id"]
    assert r.json()["url"] == f"/apps/social/{app_id}"
    r = requests.get(f"{BASE}/apps/social/{app_id}")
    assert r.status_code == 200 and "CIAO" in r.text, r.status_code
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert app_id in s["app_configs"], s["app_configs"]

    # POST /api/apps senza auth -> 401
    assert requests.post(f"{BASE}/api/apps", json={"tipo": "social"}).status_code == 401

    # tipo non valido -> 400; delete -> ok; seconda delete -> 404
    r = requests.post(f"{BASE}/api/apps", auth=AUTH, json={"tipo": "bogus"})
    assert r.status_code == 400, r.status_code
    r = requests.delete(f"{BASE}/api/apps/{app_id}", auth=AUTH)
    assert r.json().get("ok"), r.text
    r = requests.delete(f"{BASE}/api/apps/{app_id}", auth=AUTH)
    assert r.status_code == 404, r.status_code

    # ================= news tv (v2.3) =================
    # Sky TG24: default = playlist HLS ufficiale -> video hls.js
    r = requests.get(f"{BASE}/apps/news_tv?channel=skytg24")
    assert r.status_code == 200, r.status_code
    assert 'id="tv-player"' in r.text and "hls.js" in r.text, r.text[:400]
    assert "master.m3u8" in r.text and " muted" in r.text, r.text[:400]
    # sorgente YouTube -> embed nocookie (fallback)
    r = requests.get(f"{BASE}/apps/news_tv?channel=skytg24"
                     "&stream=UCz6E3lF72mb6uoJ-mOlNo2A")
    assert "live_stream?channel=UCz6E3lF72mb6uoJ-mOlNo2A" in r.text, \
        r.text[:400]
    # override puntuale .m3u8 anche per Sky TG24 -> hls.js
    r = requests.get(f"{BASE}/apps/news_tv?channel=skytg24"
                     "&stream=https://cdn.test/sky.m3u8")
    assert 'id="tv-player"' in r.text, r.text[:400]
    assert "https://cdn.test/sky.m3u8" in r.text, r.text[:400]
    # canale sconosciuto -> fallback skytg24, sempre 200 (pagina pubblica)
    r = requests.get(f"{BASE}/apps/news_tv?channel=bogus")
    assert r.status_code == 200 and "Sky TG24" in r.text, r.status_code
    # Rai News 24: video hls.js, stream .m3u8 nel markup
    r = requests.get(f"{BASE}/apps/news_tv?channel=rainews24&mute=0")
    assert r.status_code == 200, r.status_code
    assert 'id="tv-player"' in r.text and "hls.js" in r.text, r.text[:400]
    assert ".m3u8" in r.text, r.text[:400]
    # override puntuale della sorgente via ?stream=
    r = requests.get(f"{BASE}/apps/news_tv?channel=rainews24"
                     "&stream=https://cdn.test/custom.m3u8")
    assert "https://cdn.test/custom.m3u8" in r.text, r.text[:400]
    # impostazioni canali: 401 senza auth; con auth aggiorna config + stato
    assert requests.post(f"{BASE}/api/apps/news_tv",
                         json={"skytg24": "X"}).status_code == 401
    r = requests.post(f"{BASE}/api/apps/news_tv", auth=AUTH,
                      json={"skytg24": "UCtest1234567890abcdef"})
    assert r.json().get("ok"), r.text
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["news_tv"]["skytg24"] == "UCtest1234567890abcdef", s["news_tv"]
    # ID canale UC -> embed live_stream?channel=
    r = requests.get(f"{BASE}/apps/news_tv?channel=skytg24")
    assert "live_stream?channel=UCtest1234567890abcdef" in r.text, r.text[:400]
    # restore default .m3u8 + app news_tv salvabile come le altre
    r = requests.post(f"{BASE}/api/apps/news_tv", auth=AUTH,
                      json={"skytg24":
                            "https://hlslive-web-gcdn-skycdn-it.akamaized.net/"
                            "TACT/12221/web/master.m3u8"})
    assert r.json().get("ok"), r.text
    r = requests.get(f"{BASE}/apps/news_tv?channel=skytg24")
    assert 'id="tv-player"' in r.text and "TACT/12221" in r.text, \
        r.text[:400]
    r = requests.post(f"{BASE}/api/apps", auth=AUTH,
                      json={"nome": "News Test", "tipo": "news_tv",
                            "url": "/apps/news_tv?channel=rainews24&mute=1",
                            "params": {"channel": "rainews24", "audio": "off"}})
    assert r.json().get("ok"), r.text
    requests.delete(f"{BASE}/api/apps/{r.json()['id']}", auth=AUTH)

    # ================= youtube (v2.2) =================
    yt = socketio.Client()
    yt_events = []
    yt.on("content", lambda d: yt_events.append(d))
    yt.connect(BASE, wait_timeout=5)
    yt.emit("join_room", {"gruppo": "ytg"})
    time.sleep(0.8)

    # URL YouTube -> tipo 'youtube' rilevato server-side, niente proxy
    yt_events.clear()
    r = requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                      json={"gruppi": ["ytg"],
                            "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                            "mode": "auto"})
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    assert yt_events and yt_events[-1]["tipo"] == "youtube", yt_events
    assert yt_events[-1]["mode"] == "iframe", yt_events

    # playlist mista: video YT (60s) + immagine -> ENDED avanza subito
    yt_events.clear()
    r = requests.post(f"{BASE}/api/playlist", auth=AUTH,
                      json={"gruppo": "ytg",
                            "playlist": [
                                {"url": "https://youtu.be/dQw4w9WgXcQ",
                                 "durata_sec": 60},
                                {"url": "/media/x.png", "durata_sec": 5}]})
    assert r.json().get("ok"), r.text
    time.sleep(2.5)  # emit slide + dwell minimo 2s anti-flood
    assert yt_events and yt_events[-1]["tipo"] == "youtube", yt_events
    assert yt_events[-1]["playlist_attiva"], yt_events
    yt.emit("playlist_next", {"url": "https://youtu.be/dQw4w9WgXcQ"})
    time.sleep(1.2)
    assert yt_events[-1]["url"].startswith("/media/x.png"), yt_events

    # segnale stale (slide gia' cambiata) -> ignorato, nessun avanzamento
    yt.emit("playlist_next", {"url": "https://youtu.be/dQw4w9WgXcQ"})
    time.sleep(1.0)
    assert yt_events[-1]["url"].startswith("/media/x.png"), yt_events

    requests.post(f"{BASE}/api/playlist", auth=AUTH,
                  json={"gruppo": "ytg", "playlist": []})
    requests.delete(f"{BASE}/api/gruppo/ytg", auth=AUTH)
    yt.disconnect()

    # ================= palinsesto (v2.4) =================
    try:
        from zoneinfo import ZoneInfo
        _now = datetime.now(ZoneInfo("Europe/Rome"))
    except Exception:
        _now = datetime.now()
    gsched = "schedgrp"
    requests.post(f"{BASE}/api/gruppi", auth=AUTH, json={"nome": gsched})
    sc = socketio.Client()
    sc_events = []
    sc.on("content", lambda d: sc_events.append(d))
    sc.connect(BASE, wait_timeout=5)
    sc.emit("join_room", {"gruppo": gsched})
    time.sleep(0.8)

    # contenuto predefinito del gruppo = fallback a fine fascia
    r = requests.post(f"{BASE}/api/gruppo/{gsched}/default", auth=AUTH,
                      json={"url": "/media/fallback.png", "mode": "iframe"})
    assert r.json().get("ok"), r.text

    # dati fascia non validi -> 400
    r = requests.post(f"{BASE}/api/schedules", auth=AUTH,
                      json={"name": "bad", "target": f"group:{gsched}",
                            "content_url": "/media/x.png", "days": [9],
                            "start_time": "25:00", "end_time": "26:00"})
    assert r.status_code == 400, r.status_code

    # fascia attiva ORA (Europe/Rome) -> applicata subito al salvataggio
    st_t = (_now - timedelta(minutes=2)).strftime("%H:%M")
    et_t = (_now + timedelta(minutes=10)).strftime("%H:%M")
    if et_t <= st_t:
        et_t = "23:59"
    r = requests.post(f"{BASE}/api/schedules", auth=AUTH,
                      json={"name": "fascia test",
                            "target": f"group:{gsched}",
                            "content_url": "/media/sched.png",
                            "mode": "iframe", "days": [_now.isoweekday()],
                            "start_time": st_t, "end_time": et_t,
                            "enabled": True})
    assert r.json().get("ok"), r.text
    sid = r.json()["schedule"]["id"]
    time.sleep(1.0)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["gruppi"][gsched]["contenuto_attivo"] == "/media/sched.png", \
        s["gruppi"][gsched]
    assert sc_events and \
        sc_events[-1]["url"].startswith("/media/sched.png"), sc_events

    # 'Metti in onda' manuale: marca l'override; la STESSA fascia attiva
    # non deve riprendere il controllo (tick forzato via toggle no-op)
    r = requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                      json={"gruppi": [gsched], "url": "/media/manual.png",
                            "mode": "iframe"})
    assert r.json().get("ok"), r.text
    r = requests.post(f"{BASE}/api/schedules", auth=AUTH,
                      json={"id": sid, "enabled": True})
    assert r.json().get("ok"), r.text
    time.sleep(0.5)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["gruppi"][gsched]["contenuto_attivo"] == "/media/manual.png", \
        "il palinsesto ha sovrascritto l'override manuale"
    assert f"g:{gsched}" in s["sched_manual"], s["sched_manual"]

    # fine fascia (toggle off) -> manuale scade, torna il predefinito
    r = requests.post(f"{BASE}/api/schedules", auth=AUTH,
                      json={"id": sid, "enabled": False})
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert f"g:{gsched}" not in s["sched_manual"], s["sched_manual"]
    assert s["gruppi"][gsched]["contenuto_attivo"] == \
        "/media/fallback.png", s["gruppi"][gsched]

    # override manuale + 'Riprendi palinsesto' -> torna il predefinito
    requests.post(f"{BASE}/api/contenuto", auth=AUTH,
                  json={"gruppi": [gsched], "url": "/media/manual2.png",
                        "mode": "iframe"})
    r = requests.post(f"{BASE}/api/schedules/resume", auth=AUTH, json={})
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["gruppi"][gsched]["contenuto_attivo"] == \
        "/media/fallback.png", s["gruppi"][gsched]
    assert not s["sched_manual"], s["sched_manual"]

    # fascia su device singolo: l'override manuale vince fino al resume
    # mirato, poi la fascia applica il contenuto dedicato; a fine fascia
    # (delete) il device torna al feed del gruppo
    dev2 = socketio.Client()
    dev2_events = []
    dev2.on("content", lambda d: dev2_events.append(d))
    dev2.connect(BASE, wait_timeout=5)
    dev2.emit("join_room", {"device_id": "TV-TEST1"})
    time.sleep(0.8)
    requests.post(f"{BASE}/api/dispositivi/TV-TEST1/contenuto", auth=AUTH,
                  json={"url": "/media/devmanual.png", "mode": "iframe"})
    r = requests.post(f"{BASE}/api/schedules", auth=AUTH,
                      json={"name": "fascia dev",
                            "target": "device:TV-TEST1",
                            "content_url": "/media/devsched.png",
                            "mode": "iframe", "days": [_now.isoweekday()],
                            "start_time": st_t, "end_time": et_t,
                            "enabled": True})
    assert r.json().get("ok"), r.text
    dsid = r.json()["schedule"]["id"]
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    dc = s["dispositivi"]["TV-TEST1"].get("contenuto") or {}
    assert dc.get("url") == "/media/devmanual.png", \
        "la fascia non doveva scavaliare l'override manuale del device"
    r = requests.post(f"{BASE}/api/schedules/resume", auth=AUTH,
                      json={"target": "d:TV-TEST1"})
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    dc = s["dispositivi"]["TV-TEST1"].get("contenuto") or {}
    assert dc.get("url") == "/media/devsched.png", dc
    assert dev2_events and dev2_events[-1].get("dedicato"), dev2_events
    r = requests.delete(f"{BASE}/api/schedules/{dsid}", auth=AUTH)
    assert r.json().get("ok"), r.text
    time.sleep(0.8)
    s = requests.get(f"{BASE}/api/stato", auth=AUTH).json()
    assert s["dispositivi"]["TV-TEST1"].get("contenuto") is None, \
        s["dispositivi"]["TV-TEST1"]
    dev2.disconnect()

    # cleanup palinsesto: fascia e gruppo di test
    requests.delete(f"{BASE}/api/schedules/{sid}", auth=AUTH)
    requests.delete(f"{BASE}/api/gruppo/{gsched}", auth=AUTH)
    sc.disconnect()

    print("TUTTI I TEST PASSATI: content, cache-buster, force_reload, "
          "proxy 400/403, client count, device register(default)/identify/"
          "rename/reassign/override-dedicato/gruppi-create/"
          "group-delete-fallback/device-delete, "
          "apps routes/weather-proxy/app_configs CRUD/auth, "
          "youtube tipo/playlist_next, "
          "news_tv routes/stream-override/settings-api, "
          "broadcast-exclude-default, device-pairing-idle, "
          "schedules CRUD/toggle/resume/manual-override/"
          "fallback-gruppo/device-schedule")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"TEST FALLITO: {e!r}")
        sys.exit(1)

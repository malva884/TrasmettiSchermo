# TrasmettiShermo — Digital Signage centralizzato

Sistema self-hosted client-server per gestire contenuti (pagine web, video, immagini) su più schermi tramite browser kiosk. Push in tempo reale via WebSocket (Socket.IO rooms), nessun software proprietario sui client.

## Avvio rapido

```powershell
pip install -r requirements.txt
python main.py
```

- **Regia (admin)**: `http://<IP-SERVER>:5000/admin` — HTTP Basic Auth
- **Player (TV)**: `http://<IP-SERVER>:5000/tv/<nome_gruppo>` — il gruppo viene creato automaticamente alla prima apertura

## Configurazione (variabili d'ambiente)

| Variabile | Default | Descrizione |
|---|---|---|
| `ADMIN_USER` | `admin` | Username pannello admin |
| `ADMIN_PASSWORD` | `admin` | Password admin — **cambiarla in produzione** |
| `PORT` | `5000` | Porta del server (solo avvio dev `python main.py`) |
| `DEFAULT_CONTENT_URL` | *(vuoto)* | Contenuto assegnato ai gruppi appena creati |
| `DATA_DIR` | `./data` | Directory dati persistenti (`config.json` → `$DATA_DIR/config.json`) |
| `MEDIA_DIR` | `./media` | Directory dei media caricati, serviti su `/media/` |
| `TZ` | `Europe/Rome` | Fuso orario del palinsesto (nel Dockerfile già impostato) |

PowerShell (sessione corrente):
```powershell
$env:ADMIN_PASSWORD = "password-sicura"; python main.py
```

## Struttura

```
main.py              server Flask + SocketIO (rooms, playlist engine, API)
data/config.json     stato persistente di gruppi, device e app (auto-salvato)
media/               file caricati dal pannello admin
templates/player.html  kiosk player (/tv/<gruppo>)
templates/admin.html   pannello di regia (/admin)
templates/apps/        app fullscreen servite su /apps/<tipo>
Dockerfile           immagine produzione (Gunicorn + gevent, 1 worker)
```

## Funzionalità

- **Player kiosk** `/tv/<gruppo>`: video `autoplay loop muted playsinline`, immagini e iframe a `100vw/100vh`, sfondo nero
- **YouTube nativo** (tipo `youtube`): incolla qualsiasi URL YouTube (`watch?v=`, `youtu.be/`, `/shorts/`, `/live/`, `/embed/`, playlist `?list=`). Renderizzato via embed `youtube-nocookie` HTTPS (niente banner cookie né Mixed Content su Coolify), loop automatico (`playlist=<ID>` per i video singoli)
- **Riconnessione automatica** con backoff esponenziale; al reconnect il client re-entra nella room e riceve il contenuto corrente
- **Cache-busting** automatico (`?v=<ts>`) sui file in `/media/`
- **Refresh notturno alle 04:00** lato client (anti memory-leak su Fire TV Stick) + pulsante **Force reload** remoto
- **Fallback X-Frame-Options**: modalità `redirect` selezionabile in admin per siti che bloccano gli iframe; timeout-euristica di 12s sull'iframe. La modalità `proxy` inoltra anche le richieste `POST` (body/cookie) e inietta nelle pagine HTML un interceptor che reindirizza `fetch`/`XMLHttpRequest` interne verso `/proxy` — le bacheche interattive funzionano senza blocchi CORS mantenendo il controllo WebSocket
- **Playlist temporizzate** `[{url, durata_sec}]` gestite lato server (sincronizzate su tutti i client del gruppo), riavviate automaticamente dopo un reboot del server. Per le slide YouTube il player usa la IFrame Player API e segnala l'evento `ENDED`: la slide avanza al termine esatto del filmato (`durata_sec` resta il tempo massimo; `durata_sec: 0` = attendi solo la fine del video, cap 24h)
- **Contatore client connessi** per gruppo, in tempo reale nel pannello admin
- **Targeting**: singolo gruppo, sottoinsieme (checkbox) o broadcast "Tutti"
- **Guida setup dispositivi** in `/admin` (pulsante in topbar): URL player dinamico + QR code, istruzioni kiosk per Fire TV Stick, Android/Google TV, Samsung/LG, Mini-PC Windows, Raspberry/Linux e tablet

## Palinsesto

La sezione **Palinsesto** dell'admin gestisce fasce orarie persistenti
(`config.json → schedules`): ogni fascia ha nome, destinatario (tutti i gruppi,
singolo gruppo o singolo schermo), contenuto (app salvata, media o URL
personalizzato), giorni della settimana, orario `start`–`end` e stato
attiva/disattiva.

Un worker in background valuta le fasce **ogni 30 secondi** sull'orologio
`Europe/Rome` e manda in onda il contenuto previsto con lo stesso evento
Socket.IO `content` di "Metti in onda" (per i device: override dedicato nella
room `dev:<id>`).

Regole di arbitrio:

- **Override manuale**: "Metti in onda", playlist e contenuto dedicato ai
  device marcano il target come manuale. Il contenuto manuale resta attivo
  fino al **cambio della fascia attiva** (inizio/fine fascia o cambio
  contenuto) oppure finché non si preme **"Riprendi palinsesto"** nel banner
  ambra in cima alla vista (o `POST /api/schedules/resume`).
- **Fallback a fine fascia**: se il gruppo ha un **contenuto predefinito**
  (`gruppo → menu ⋯ → "Predefinito = contenuto attuale"`, salvato in
  `default_content`) viene rimandato in onda al termine della fascia;
  altrimenti resta l'ultimo contenuto. Per un device, a fine fascia
  l'override dedicato viene rimosso e torna al feed del gruppo.
- In caso di fasce sovrapposte sullo stesso target vince la prima nell'elenco.

## App & Widget

Ogni app è una rotta HTML5 fullscreen servita dal server: il player la renderizza
in iframe come un normale contenuto `url` (fullscreen, slide di playlist o area
dello split-screen).

| App | Rotta | Parametri |
|---|---|---|
| Orologio | `GET /apps/clock` | `?title=..&theme=dark\|light&show_seconds=1` — 100% client-side, funziona offline |
| Meteo | `GET /apps/weather` | `?city=..&lat=..&lon=..` — dati via proxy interno `/api/apps/weather_data` (Open-Meteo, cache 30 min, fallback stale) |
| Traffico | `GET /apps/traffic` | `?lat=..&lon=..&zoom=..` — Google Maps embed layer traffico, auto-refresh 5 min |
| Social | `GET /apps/social/<widget_id>` | render dello `html_snippet` salvato in `config.json → app_configs`, auto-reload 15 min |
| Diretta News TV | `GET /apps/news_tv` | `?channel=skytg24\|rainews24&mute=1\|0&badge=1\|0&stream=..` — Sky TG24 e Rai News 24 via hls.js riprodotto nel browser del player (usa l'IP della sede → niente geo-block del server né blocchi di embed YouTube), auto-reconnect 5s, badge LIVE sovrapposto; per Sky TG24 una sorgente YouTube usa comunque l'embed `youtube-nocookie`. `mute=0` tenta l'autoplay con audio: se il browser lo blocca il video resta muto e si attiva al primo tocco/click/tasto sullo schermo (tap-to-unmute via `postMessage` dal player) |
| Split-Screen | `GET /apps/split` | `?main=<url>&side=<url>` — grid 75%/25% (video/immagine/app + widget laterale) |

Nel pannello admin la sezione **App & Widget** offre card di configurazione
(orologio, meteo con geocoding automatico città→lat/lon, traffico, embed social,
diretta news TV, split-screen). Le app salvate compaiono come asset: trasmissibili con un clic,
inseribili nelle playlist e nei contenuti dedicati ai device. Il salvataggio
degli `html_snippet` avviene solo via `/api/apps` protetta da Basic Auth.

Per **Diretta News TV** la card permette di scegliere il canale
(Sky TG24 / Rai News 24), l'audio (muto/attivo) e la sorgente stream:
URL `.m3u8` (consigliato — per Sky TG24 è quello ufficiale del player web
di tg24.sky.it, con token `hdnts` a lunga scadenza da rigenerare se
scade) oppure ID diretta YouTube / ID canale `UC…` per Sky.
La sorgente viene salvata come default globale in `config.json → news_tv`
(vale per tutte le app dello stesso canale); `?stream=` sull'URL permette
un override puntuale senza toccare il default.

## Deployment

### Coolify / Docker (produzione)

Il `Dockerfile` avvia Gunicorn con `GeventWebSocketWorker` (**un solo worker**: stato e room Socket.IO vivono in memoria di processo, nessun broker Redis richiesto). Socket.IO usa `async_mode=gevent`.

**Volumi da montare su storage persistente** (sopravvivono a redeploy/restart):

| Path nel container | Contenuto | Env |
|---|---|---|
| `/app/data` | `config.json` (gruppi, device, playlist, app) | `DATA_DIR` |
| `/app/static/media` | media caricati dall'admin | `MEDIA_DIR` |

**Checklist Coolify**:

1. Build pack **Dockerfile**, porta interna `5000`, dominio HTTPS assegnato da Coolify (wss:// gestito dal proxy automaticamente).
2. Impostare `ADMIN_PASSWORD` (e opzionalmente `ADMIN_USER`) nelle variabili d'ambiente.
3. Montare i due volumi della tabella sopra.
4. Healthcheck: `GET /health` → `200 {"status":"ok"}` (pubblico, senza Basic Auth; il Dockerfile include già un `HEALTHCHECK`).

Il container gira in **UTC** di default: il Dockerfile imposta `TZ=Europe/Rome`
e installa `tzdata`, quindi il palinsesto segue l'ora italiana anche su
Coolify. Per un fuso diverso basta ridefinire `TZ`.

Note: upload fino a **250 MB** (`MAX_CONTENT_LENGTH`); se davanti c'è un ulteriore proxy (Nginx/Traefik custom), allineare `client_max_body_size`/limiti upload. Il client Socket.IO usa `transports:["websocket","polling"]` sull'origine corrente — niente Mixed Content in HTTPS.

### Rete
Assegnare al server un **IP statico** via DHCP reservation sul router. I player puntano a `http://<IP>:5000/tv/<gruppo>`.

### Servizio — Windows (Task Scheduler)
```powershell
schtasks /create /tn "TrasmettiShermo" /tr "\"C:\path\to\.venv\Scripts\python.exe\" \"C:\path\to\main.py\"" /sc onstart /ru SYSTEM
```
Oppure usare **NSSM**: `nssm install TrasmettiShermo` → Path: python.exe, Arguments: main.py, Startup dir: cartella progetto.

### Servizio — Linux (systemd)
```ini
# /etc/systemd/system/signage.service
[Unit]
Description=Digital Signage Server
After=network.target

[Service]
WorkingDirectory=/opt/signage
Environment=ADMIN_PASSWORD=password-sicura
ExecStart=/opt/signage/.venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl enable --now signage
```

## Configurazione client — Amazon Fire TV Stick

1. Aprire **Amazon Silk** → `http://<IP-SERVER>:5000/tv/<gruppo>` → modalità **Schermo Intero**
2. Impostazioni → Schermo e suoni → **Salvaschermo → Mai**
3. Alimentazione da **alimentatore a muro** (non USB del TV: toglierebbe tensione allo spegnimento)

## API REST (autenticate)

| Metodo | Endpoint | Body | Descrizione |
|---|---|---|---|
| GET | `/api/stato` | — | Gruppi, client connessi, media |
| POST | `/api/contenuto` | `{gruppi:"all"\|[..], url, mode}` | Invia contenuto |
| POST | `/api/upload` | multipart `file` | Upload in `/media/` |
| POST | `/api/media/delete` | `{file}` | Elimina media |
| POST | `/api/reload` | `{gruppi}` | Force reload remoto |
| POST | `/api/playlist` | `{gruppo, playlist:[{url,durata_sec}]}` | Avvia/ferma playlist |
| POST | `/api/apps` | `{id?, nome, tipo, url?, params?, html_snippet?}` | Salva app in `app_configs` |
| DELETE | `/api/apps/<id>` | — | Elimina app salvata |
| GET | `/api/apps/geocode` | `?city=..` | Geocoding Open-Meteo (admin) |
| GET | `/api/apps/weather_data` | `?lat=..&lon=..` | Proxy meteo pubblico, cache 30 min |
| POST | `/api/apps/news_tv` | `{skytg24?, rainews24?}` | Sorgenti dirette in `config.json → news_tv` |
| POST | `/api/schedules` | `{id?, name, target, content_url, mode?, days:[1..7], start_time, end_time, enabled?}` | Crea/aggiorna fascia; con solo `{id, enabled}` = toggle |
| DELETE | `/api/schedules/<id>` | — | Elimina fascia |
| POST | `/api/schedules/resume` | `{target?}` | Annulla override manuali e rivaluta subito il palinsesto |
| POST | `/api/gruppo/<nome>/default` | `{use_current?}`/`{url, mode?}`/`{url:null}` | Imposta/rimuove il contenuto predefinito (fallback) |
| DELETE | `/api/gruppo/<nome>` | — | Elimina gruppo |

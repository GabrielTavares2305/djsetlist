"""
DJ Set Analyzer - Backend Server
=================================
Este servidor Flask gerencia todo o pipeline:
1. Recebe a URL do frontend
2. Baixa o áudio com yt-dlp
3. Divide o áudio em segmentos de 30s com ffmpeg
4. Envia cada segmento ao Shazam para identificação
5. Salva os resultados no banco SQLite
6. Envia progresso em tempo real via Server-Sent Events (SSE)
"""

import os
import sys
import json
import time
import sqlite3
import asyncio
import threading
import subprocess
import hashlib
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS

# ── Caminhos do projeto ──────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent.parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
SEGMENTS_DIR  = BASE_DIR / "segments"
DB_PATH       = BASE_DIR / "db" / "setlist.db"

DOWNLOADS_DIR.mkdir(exist_ok=True)
SEGMENTS_DIR.mkdir(exist_ok=True)
DB_PATH.parent.mkdir(exist_ok=True)

app = Flask(__name__)
CORS(app)

# Fila de progresso por job_id
progress_queues: dict[str, list] = {}
job_status: dict[str, str] = {}  # "running" | "done" | "error"


# ── Banco de Dados ─────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """
    Cria as tabelas se não existirem.
    - sets:   cada URL analisada vira um registro
    - tracks: cada música identificada vinculada ao set
    """
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url         TEXT NOT NULL,
            title       TEXT,
            platform    TEXT,
            analyzed_at TEXT NOT NULL,
            duration_s  INTEGER,
            status      TEXT DEFAULT 'analyzing'
        );

        CREATE TABLE IF NOT EXISTS tracks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            set_id      INTEGER NOT NULL REFERENCES sets(id),
            position    INTEGER NOT NULL,
            timestamp_s INTEGER NOT NULL,
            title       TEXT,
            artist      TEXT,
            album       TEXT,
            genre       TEXT,
            cover_url   TEXT,
            shazam_id   TEXT,
            confidence  REAL,
            identified  INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()


init_db()


# ── Utilitários ────────────────────────────────────────────────────────────
def push_event(job_id: str, event_type: str, data: dict):
    """Empurra um evento SSE na fila do job."""
    if job_id not in progress_queues:
        progress_queues[job_id] = []
    progress_queues[job_id].append({
        "type": event_type,
        "data": json.dumps(data),
        "ts":   time.time()
    })


def detect_platform(url: str) -> str:
    if "soundcloud.com" in url:
        return "SoundCloud"
    if "youtube.com" in url or "youtu.be" in url:
        return "YouTube"
    return "Unknown"


def format_time(seconds: int) -> str:
    """Converte segundos em HH:MM:SS ou MM:SS."""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


# ── Download com yt-dlp ────────────────────────────────────────────────────
def download_audio(url: str, job_id: str) -> tuple[Path, str, int]:
    """
    Baixa o áudio em dois passos:
    1. Busca título e duração sem baixar (--skip-download)
    2. Baixa o áudio de verdade sem --print
    Separar os dois evita o bug onde --print + download
    juntos fazem o yt-dlp descartar o arquivo.
    """
    push_event(job_id, "status", {"message": "📥 Buscando informações...", "step": 1})

    safe_id         = hashlib.md5(url.encode()).hexdigest()[:12]
    output_template = str(DOWNLOADS_DIR / f"{safe_id}.%(ext)s")

    # ── Passo 1: busca título e duração ──────────────────────────────
    info_cmd = [
        sys.executable, "-m", "yt_dlp",
        "--skip-download",
        "--js-runtimes", "deno",
        "--print", "%(title)s|||%(duration)s",
        url
    ]
    info_result = subprocess.run(info_cmd, capture_output=True, text=True, timeout=60)

    title    = "Set sem título"
    duration = 0
    for line in info_result.stdout.strip().split("\n"):
        if "|||" in line:
            parts = line.split("|||")
            title = parts[0].strip() or title
            try:
                duration = int(float(parts[1].strip()))
            except (ValueError, IndexError):
                pass

    push_event(job_id, "status", {"message": f"📥 Baixando: {title}", "step": 1})

    # ── Passo 2: baixa o áudio ────────────────────────────────────────
    dl_cmd = [
        sys.executable, "-m", "yt_dlp",
        "--extract-audio",
        "--audio-format",  "mp3",
        "--audio-quality", "0",
        "--output",        output_template,
        "--no-playlist",
        "--js-runtimes",   "deno",
        url
    ]
    dl_result = subprocess.run(dl_cmd, capture_output=True, text=True, timeout=600)

    if dl_result.returncode != 0:
        raise RuntimeError(f"Falha no download: {dl_result.stderr[-500:]}")

    # Aguarda até 15 segundos o arquivo aparecer no disco
    mp3_path = DOWNLOADS_DIR / f"{safe_id}.mp3"
    for _ in range(15):
        if mp3_path.exists():
            break
        candidates = list(DOWNLOADS_DIR.glob(f"{safe_id}.*"))
        if candidates:
            mp3_path = candidates[0]
            break
        time.sleep(1)
    else:
        existentes = [f.name for f in DOWNLOADS_DIR.iterdir()]
        raise FileNotFoundError(
            f"Arquivo {safe_id}.mp3 não encontrado após download. "
            f"Arquivos na pasta: {existentes}"
        )

    push_event(job_id, "status", {
        "message":  f"✅ Download completo: {title}",
        "step":     1,
        "title":    title,
        "duration": duration
    })

    return mp3_path, title, duration


# ── Segmentação com ffmpeg ─────────────────────────────────────────────────
def segment_audio(audio_path: Path, job_id: str, segment_duration: int = 30) -> list[tuple[Path, int]]:
    """
    Divide o áudio em pedaços de `segment_duration` segundos usando ffmpeg.
    Retorna lista de (caminho_segmento, timestamp_inicio_em_segundos).

    Por que 30s? Tempo ideal para o Shazam identificar mesmo em mixes
    com crossfade, sem ser redundante demais.
    """
    push_event(job_id, "status", {"message": "✂️ Segmentando áudio...", "step": 2})

    # Obtém duração total com ffprobe
    probe_cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of",           "default=noprint_wrappers=1:nokey=1",
        str(audio_path)
    ]
    probe_result   = subprocess.run(probe_cmd, capture_output=True, text=True)
    total_duration = int(float(probe_result.stdout.strip() or "0"))

    seg_dir = SEGMENTS_DIR / audio_path.stem
    seg_dir.mkdir(exist_ok=True)

    segments  = []
    timestamp = 0
    seg_index = 0

    while timestamp < total_duration:
        seg_path = seg_dir / f"seg_{seg_index:04d}.mp3"

        cmd = [
            "ffmpeg", "-y",
            "-ss",     str(timestamp),
            "-t",      str(segment_duration),
            "-i",      str(audio_path),
            "-acodec", "libmp3lame",
            "-ar",     "44100",
            "-ab",     "128k",
            str(seg_path)
        ]
        subprocess.run(cmd, capture_output=True, timeout=60)

        if seg_path.exists() and seg_path.stat().st_size > 1000:
            segments.append((seg_path, timestamp))

        timestamp += segment_duration
        seg_index += 1

    push_event(job_id, "status", {
        "message":        f"✅ {len(segments)} segmentos criados",
        "step":           2,
        "total_segments": len(segments)
    })

    return segments


# ── Identificação com Shazam ───────────────────────────────────────────────
async def identify_segment_async(seg_path: Path) -> dict | None:
    """
    Envia um segmento ao Shazam.
    Retorna dict com metadados ou None se não identificado.
    """
    try:
        from shazamio import Shazam
        shazam = Shazam()
        result = await shazam.recognize(str(seg_path))

        if not result.get("matches"):
            return None

        track = result.get("track", {})
        if not track:
            return None

        sections = track.get("sections", [])
        metadata = {}
        for section in sections:
            if section.get("type") == "SONG":
                for meta in section.get("metadata", []):
                    metadata[meta.get("title", "").lower()] = meta.get("text", "")

        images = track.get("images", {})
        cover  = images.get("coverarthq") or images.get("coverart") or ""

        return {
            "title":      track.get("title",    "Desconhecida"),
            "artist":     track.get("subtitle", "Desconhecido"),
            "album":      metadata.get("album", ""),
            "genre":      track.get("genres", {}).get("primary", ""),
            "cover_url":  cover,
            "shazam_id":  track.get("key", ""),
            "confidence": result.get("matches", [{}])[0].get("frequencyskew", 0)
        }
    except Exception:
        return None


def identify_segment(seg_path: Path) -> dict | None:
    """
    Wrapper síncrono para a função async do Shazam.
    Cria um novo event loop a cada chamada para evitar o erro
    'ProactorEventLoop' que ocorre no Windows com asyncio.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(identify_segment_async(seg_path))
    finally:
        loop.close()


# ── Pipeline Principal ─────────────────────────────────────────────────────
def run_analysis_pipeline(url: str, job_id: str, set_id: int):
    """
    Executa o pipeline completo numa thread separada para não
    bloquear o servidor Flask enquanto analisa.
    """
    conn = get_db()

    try:
        # 1. Download
        audio_path, title, duration = download_audio(url, job_id)
        conn.execute(
            "UPDATE sets SET title=?, duration_s=? WHERE id=?",
            (title, duration, set_id)
        )
        conn.commit()

        # 2. Segmentação
        segments = segment_audio(audio_path, job_id)
        total    = len(segments)

        push_event(job_id, "status", {
            "message": "🎵 Identificando músicas com Shazam...",
            "step":    3,
            "total":   total
        })

        # 3. Identificação segmento a segmento
        last_shazam_id   = None
        track_position   = 0
        identified_count = 0

        for i, (seg_path, timestamp) in enumerate(segments):
            push_event(job_id, "progress", {
                "current":   i + 1,
                "total":     total,
                "timestamp": timestamp,
                "percent":   int((i + 1) / total * 100)
            })

            result = identify_segment(seg_path)

            if result:
                shazam_id = result["shazam_id"]

                # Deduplicação: mesma música em segmentos consecutivos = 1 entrada
                # Evita que uma faixa de 3min apareça 6 vezes (6 segmentos de 30s)
                if shazam_id and shazam_id == last_shazam_id:
                    continue

                last_shazam_id   = shazam_id
                track_position  += 1
                identified_count += 1

                conn.execute("""
                    INSERT INTO tracks
                    (set_id, position, timestamp_s, title, artist, album,
                     genre, cover_url, shazam_id, confidence, identified)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1)
                """, (
                    set_id, track_position, timestamp,
                    result["title"], result["artist"], result["album"],
                    result["genre"], result["cover_url"], result["shazam_id"],
                    result["confidence"]
                ))
                conn.commit()

                # Envia a faixa ao frontend em tempo real
                push_event(job_id, "track", {
                    "position":    track_position,
                    "timestamp_s": timestamp,
                    "timestamp":   format_time(timestamp),
                    "title":       result["title"],
                    "artist":      result["artist"],
                    "album":       result["album"],
                    "genre":       result["genre"],
                    "cover_url":   result["cover_url"],
                })
            else:
                last_shazam_id = None  # reset para detectar próxima música

            # Pausa entre requisições ao Shazam (evita rate limiting)
            time.sleep(1.2)

        # 4. Finalização
        conn.execute("UPDATE sets SET status=? WHERE id=?", ("done", set_id))
        conn.commit()

        push_event(job_id, "done", {
            "message":      f"✅ Análise completa! {identified_count} músicas identificadas.",
            "set_id":       set_id,
            "total_tracks": identified_count
        })
        job_status[job_id] = "done"

    except Exception as e:
        conn.execute("UPDATE sets SET status=? WHERE id=?", ("error", set_id))
        conn.commit()
        push_event(job_id, "error", {"message": str(e)})
        job_status[job_id] = "error"
    finally:
        conn.close()


# ── Rotas da API ───────────────────────────────────────────────────────────
@app.route("/api/analyze", methods=["POST"])
def start_analysis():
    data = request.get_json()
    url  = data.get("url", "").strip()

    if not url:
        return jsonify({"error": "URL inválida"}), 400
    if not ("youtube.com" in url or "youtu.be" in url or "soundcloud.com" in url):
        return jsonify({"error": "Apenas YouTube e SoundCloud são suportados"}), 400

    job_id = hashlib.md5(f"{url}{time.time()}".encode()).hexdigest()[:16]
    progress_queues[job_id] = []
    job_status[job_id]      = "running"

    conn   = get_db()
    cursor = conn.execute(
        "INSERT INTO sets (url, platform, analyzed_at, status) VALUES (?,?,?,?)",
        (url, detect_platform(url), datetime.now().isoformat(), "analyzing")
    )
    set_id = cursor.lastrowid
    conn.commit()
    conn.close()

    thread = threading.Thread(
        target=run_analysis_pipeline,
        args=(url, job_id, set_id),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id, "set_id": set_id})


@app.route("/api/progress/<job_id>")
def stream_progress(job_id: str):
    """
    Server-Sent Events: mantém conexão aberta e empurra eventos
    ao frontend conforme a análise avança. Muito mais eficiente
    que polling (ficar pedindo atualização de 1 em 1 segundo).
    """
    def generate():
        last_index = 0
        timeout    = 0

        while job_status.get(job_id) == "running" or timeout < 5:
            queue = progress_queues.get(job_id, [])
            if last_index < len(queue):
                for event in queue[last_index:]:
                    yield f"event: {event['type']}\ndata: {event['data']}\n\n"
                last_index = len(queue)

            if job_status.get(job_id) in ("done", "error"):
                timeout += 1

            time.sleep(0.5)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/api/info", methods=["POST"])
def get_info():
    """
    Busca título, thumbnail e duração de uma URL
    sem fazer o download do áudio.
    Usado pelo frontend para mostrar o preview antes de analisar.
    """
    data = request.get_json()
    url  = data.get("url", "").strip()

    if not url:
        return jsonify({"error": "URL inválida"}), 400

    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--skip-download",
        "--js-runtimes", "deno",
        "--print", "%(title)s|||%(duration)s|||%(thumbnail)s",
        url
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        line   = result.stdout.strip().split("\n")[0]
        parts  = line.split("|||")

        title     = parts[0].strip() if len(parts) > 0 else "Sem título"
        duration  = int(float(parts[1].strip())) if len(parts) > 1 else 0
        thumbnail = parts[2].strip() if len(parts) > 2 else ""

        return jsonify({
            "title":     title,
            "duration":  duration,
            "thumbnail": thumbnail,
            "platform":  detect_platform(url)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library")
def get_library():
    conn = get_db()
    sets = conn.execute("""
        SELECT s.*, COUNT(t.id) as track_count
        FROM sets s
        LEFT JOIN tracks t ON t.set_id = s.id AND t.identified = 1
        WHERE s.status = 'done'
        GROUP BY s.id
        ORDER BY s.analyzed_at DESC
    """).fetchall()

    result = []
    for s in sets:
        s_dict = dict(s)
        tracks = conn.execute("""
            SELECT * FROM tracks
            WHERE set_id = ? AND identified = 1
            ORDER BY position
        """, (s["id"],)).fetchall()
        s_dict["tracks"] = [dict(t) for t in tracks]
        result.append(s_dict)

    conn.close()
    return jsonify(result)


@app.route("/api/set/<int:set_id>", methods=["DELETE"])
def delete_set(set_id: int):
    conn = get_db()
    conn.execute("DELETE FROM tracks WHERE set_id=?", (set_id,))
    conn.execute("DELETE FROM sets    WHERE id=?",    (set_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("🎧 DJ Set Analyzer - Backend rodando em http://localhost:5055")
    app.run(host="127.0.0.1", port=5055, debug=False, threaded=True)
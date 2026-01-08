import os
import re
import json
import time
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import whisper  # openai-whisper

# =========================
# 설정
# =========================

REPORT_FILE = "report.txt"
UC_IDS_FILE = "uc_ids.txt"

RECENT_HOURS = 12                 # ✅ 최근 12시간
SEGMENT_SECONDS = 15 * 60         # 15분 분할
MAX_RETRIES = 2                   # 무조건 재다운 2회 => 총 3회
WHISPER_MODEL = "base"            # GitHub Actions CPU 기준

WORKDIR = "_work"
AUDIODIR = os.path.join(WORKDIR, "audio")
SEGDIR = os.path.join(WORKDIR, "segments")
CACHE_DIR = os.path.join(WORKDIR, "cache")
UC_CACHE_PATH = os.path.join(CACHE_DIR, "uc_cache.json")

# ✅ 사람은 handle만 넣으면 됨
CHANNELS = {
    "Bloomberg": "https://www.youtube.com/@BloombergTV",
    "Meet Kevin": "https://www.youtube.com/@MeetKevin",
    "오선의 미국 증시": "https://www.youtube.com/@futuresnow",
    "설명왕 테이버": "https://www.youtube.com/@taver",
    "내일은 투자왕 - 김단테": "https://www.youtube.com/@kimdante",
    "소수몽키": "https://www.youtube.com/@sosumonkey",
}

# ✅ 가장 안정성 높은 UC ID 오버라이드(내부 처리용)
# - handle/videos가 404 뜨는 채널을 구조적으로 방지
UC_OVERRIDES = {
    "Bloomberg": "UCIALMKvObZNtJ6AmdCLP7Lg",
    "Meet Kevin": "UCUvvj5lwue7PspotMDjk5UA",
    "오선의 미국 증시": "UC_JJ_NhRqPKcIOj5Ko3W_3w",
    "설명왕 테이버": "UCOio3vyYLWiKlHSYRKW-9UA",
    "내일은 투자왕 - 김단테": "UCKTMvIu9a4VGSrpWy-8bUrQ",
    "소수몽키": "UCC3yfxS5qC6PCwDzetUuEWg",
}


# =========================
# 공통 유틸
# =========================

def run(cmd: List[str], timeout: Optional[int] = None) -> Tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    return p.returncode, p.stdout, p.stderr


def safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:120] if len(name) > 120 else name


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def kst_now_str() -> str:
    return (utc_now() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M (KST)")


def label_error(stderr_or_msg: str) -> str:
    s = (stderr_or_msg or "").lower()
    if "http error 404" in s or "requested entity was not found" in s or "404" in s:
        return "HTTP_404"
    if "http error 403" in s or "403 forbidden" in s:
        return "HTTP_403"
    if "http error 429" in s or "too many requests" in s or "429" in s:
        return "HTTP_429"
    if "not available in your country" in s or "geoblock" in s:
        return "GEO_BLOCK"
    if "age-restricted" in s or "confirm your age" in s:
        return "AGE_RESTRICTED"
    if "private video" in s:
        return "PRIVATE_VIDEO"
    if "ffmpeg" in s and ("failed" in s or "error" in s or "invalid" in s):
        return "FFMPEG_FAIL"
    if "timeout" in s or "timed out" in s:
        return "TIMEOUT"
    if "live" in s and ("is currently live" in s or "livestream" in s):
        return "LIVE"
    return "UNKNOWN"


def stage_reason_from_exception(msg: str) -> Tuple[str, str]:
    m = msg or ""
    if "channel_id" in m or "채널" in m:
        return "FETCH", "채널 메타/ID 조회 실패"
    if "오디오 다운로드 실패" in m:
        return "DOWNLOAD", "오디오 다운로드 실패(차단/접근제한/포맷)"
    if "ffmpeg 정규화 실패" in m:
        return "DOWNLOAD", "오디오 정규화 실패(ffmpeg/코덱)"
    if "ffmpeg 분할 실패" in m:
        return "SPLIT", "오디오 분할 실패(ffmpeg segment)"
    if "Whisper 결과 텍스트" in m:
        return "TRANSCRIBE", "Whisper 텍스트 결과 비어 있음"
    return "TRANSCRIBE", "처리 중 알 수 없는 오류"


def make_channel_videos_url(uc_id: str) -> str:
    return f"https://www.youtube.com/channel/{uc_id}/videos"


# =========================
# UC 캐시 (선택)
# =========================

def load_uc_cache() -> Dict[str, str]:
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(UC_CACHE_PATH):
        try:
            with open(UC_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_uc_cache(cache: Dict[str, str]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(UC_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# =========================
# (A방법) 종료된 라이브 포함 + 최근 12시간: timestamp 기반 선택
# =========================

def pick_latest_content_within_12h(channel_videos_url: str) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    """
    반환: (video_url, title, timestamp(int seconds), live_status)
    - 종료된 라이브(was_live) 포함
    - 진행 중 라이브(is_live) 제외
    - timestamp(초) 기준 최근 12시간 필터
    """
    # --flat-playlist 쓰면 timestamp가 누락되는 케이스가 있어 "비-flat"으로 가져옵니다.
    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--playlist-end", "15",   # 최신 15개만 확인(빠르고 충분)
        channel_videos_url
    ]
    rc, out, err = run(cmd, timeout=240)
    if rc != 0:
        raise RuntimeError(f"yt-dlp 채널 조회 실패({label_error(err)}): {err[-600:]}")

    data = json.loads(out)
    entries = data.get("entries") or []

    now_ts = int(utc_now().timestamp())
    window = RECENT_HOURS * 3600

    best = None  # (ts, url, title, live_status)

    for e in entries:
        if not isinstance(e, dict):
            continue

        vid = e.get("id")
        if not vid:
            continue

        title = e.get("title") or ""
        live_status = e.get("live_status") or "unknown"  # not_live / was_live / is_live / is_upcoming / unknown

        # ✅ 진행 중 라이브는 제외
        if live_status in ("is_live", "is_upcoming"):
            continue

        ts = e.get("timestamp") or e.get("release_timestamp")
        if not ts:
            continue

        # ✅ 최근 12시간 필터
        if now_ts - int(ts) > window:
            continue

        url = e.get("webpage_url") or f"https://www.youtube.com/watch?v={vid}"

        if (best is None) or (int(ts) > best[0]):
            best = (int(ts), url, title, live_status)

    if not best:
        return None, None, None, None

    return best[1], best[2], best[0], best[3]


# =========================
# 오디오 다운로드 + 전처리 + 분할 + Whisper
# =========================

def download_audio(video_url: str, out_base: str) -> str:
    outtmpl = os.path.join(AUDIODIR, f"{out_base}.%(ext)s")
    cmd = [
        "yt-dlp",
        "-f", "bestaudio/best",
        "--no-playlist",
        "--no-progress",
        "-o", outtmpl,
        video_url
    ]
    rc, out, err = run(cmd, timeout=1200)
    if rc != 0:
        raise RuntimeError(f"오디오 다운로드 실패({label_error(err)}): {err[-600:]}")

    for fn in os.listdir(AUDIODIR):
        if fn.startswith(out_base + "."):
            return os.path.join(AUDIODIR, fn)

    raise RuntimeError("오디오 파일이 생성되지 않음(FILE_NOT_FOUND)")


def normalize_to_wav_mono16k(input_audio: str, out_wav: str) -> None:
    cmd = ["ffmpeg", "-y", "-i", input_audio, "-ar", "16000", "-ac", "1", "-vn", out_wav]
    rc, out, err = run(cmd, timeout=900)
    if rc != 0:
        raise RuntimeError(f"ffmpeg 정규화 실패({label_error(err)}): {err[-600:]}")


def split_wav(input_wav: str, seg_prefix: str) -> List[str]:
    os.makedirs(SEGDIR, exist_ok=True)
    prefix = os.path.join(SEGDIR, seg_prefix)

    cmd = [
        "ffmpeg", "-y", "-i", input_wav,
        "-f", "segment",
        "-segment_time", str(SEGMENT_SECONDS),
        "-c", "copy",
        f"{prefix}_%03d.wav"
    ]
    rc, out, err = run(cmd, timeout=900)
    if rc != 0:
        raise RuntimeError(f"ffmpeg 분할 실패({label_error(err)}): {err[-600:]}")

    segs = sorted(
        os.path.join(SEGDIR, f) for f in os.listdir(SEGDIR)
        if f.startswith(os.path.basename(prefix) + "_") and f.endswith(".wav")
    )
    return segs if segs else [input_wav]


def transcribe_segments(model, segments: List[str]) -> str:
    texts: List[str] = []
    for seg in segments:
        result = model.transcribe(seg, fp16=False, temperature=0)
        text = (result.get("text") or "").strip()
        if text:
            texts.append(text)
    return "\n".join(texts).strip()


# =========================
# 결과 구조
# =========================

@dataclass
class ChannelResult:
    channel: str
    status: str  # SUCCESS / NO_VIDEO / FAILED
    channel_url: str

    uc_id: Optional[str] = None
    resolved_videos_url: Optional[str] = None

    video_url: Optional[str] = None
    video_title: Optional[str] = None
    timestamp: Optional[int] = None
    live_status: Optional[str] = None

    attempts: int = 0

    failed_stage: Optional[str] = None
    fail_label: Optional[str] = None
    reason: Optional[str] = None
    error_detail: Optional[str] = None

    attempt_fail_logs: List[str] = field(default_factory=list)
    transcript_chars: int = 0


# =========================
# 채널 처리: UC 오버라이드(최우선) + 종료 라이브 포함 + 12h
# =========================

def process_channel(name: str, url: str, uc_cache: Dict[str, str], whisper_model) -> ChannelResult:
    res = ChannelResult(channel=name, status="FAILED", channel_url=url)

    # 1) UC ID 결정 (오버라이드가 있으면 그게 최우선 = 가장 안정)
    uc_id = UC_OVERRIDES.get(name)
    if not uc_id:
        # 혹시 오버라이드가 없는 채널이 생기면 캐시로도 대응 가능하게 남겨둠
        norm = url.replace("m.youtube.com", "www.youtube.com").rstrip("/")
        if norm in uc_cache:
            uc_id = uc_cache[norm]
        else:
            # 오버라이드 없는 채널은 여기서 실패로 처리(현재는 모두 오버라이드 있음)
            res.status = "FAILED"
            res.failed_stage = "FETCH"
            res.fail_label = "UC_ID_MISSING"
            res.reason = "UC ID 오버라이드가 없어 안정 처리 불가"
            return res

    res.uc_id = uc_id
    res.resolved_videos_url = make_channel_videos_url(uc_id)

    # 2) 최신 콘텐츠 선택(최근 12h, 종료된 라이브 포함)
    try:
        vurl, title, ts, live_status = pick_latest_content_within_12h(res.resolved_videos_url)
        if not vurl:
            res.status = "NO_VIDEO"
            res.reason = f"최근 {RECENT_HOURS}시간 내 '종료된 라이브/업로드' 콘텐츠 없음"
            return res
        res.video_url = vurl
        res.video_title = title
        res.timestamp = ts
        res.live_status = live_status
    except Exception as e:
        msg = str(e)
        res.status = "FAILED"
        res.failed_stage = "FETCH"
        res.fail_label = label_error(msg)
        res.reason = "채널 콘텐츠 조회 실패"
        res.error_detail = msg[-900:]
        return res

    # 3) 오디오→분할→Whisper (무조건 3회 재다운)
    base = safe_filename(f"{name}_{res.timestamp or 'notime'}")

    final_stage = None
    final_label = None
    final_reason = None
    final_detail = None

    for attempt in range(MAX_RETRIES + 1):
        res.attempts = attempt + 1
        try:
            os.makedirs(AUDIODIR, exist_ok=True)
            os.makedirs(SEGDIR, exist_ok=True)

            # 무조건 재다운 보장: 잔여물 삭제
            for fn in list(os.listdir(AUDIODIR)):
                if fn.startswith(base):
                    try:
                        os.remove(os.path.join(AUDIODIR, fn))
                    except:
                        pass
            for fn in list(os.listdir(SEGDIR)):
                if fn.startswith(base):
                    try:
                        os.remove(os.path.join(SEGDIR, fn))
                    except:
                        pass

            audio_path = download_audio(res.video_url, base)
            wav_path = os.path.join(AUDIODIR, f"{base}_clean.wav")
            normalize_to_wav_mono16k(audio_path, wav_path)

            segs = split_wav(wav_path, base)
            transcript = transcribe_segments(whisper_model, segs)

            if not transcript:
                raise RuntimeError("Whisper 결과 텍스트가 비어 있음(WHISPER_EMPTY)")

            res.status = "SUCCESS"
            res.transcript_chars = len(transcript)
            res.failed_stage = None
            res.fail_label = None
            res.reason = f"성공 (총 {res.attempts}회 시도)" if res.attempts > 1 else "성공"
            res.error_detail = None
            return res

        except Exception as e:
            msg = str(e)
            stage, reason = stage_reason_from_exception(msg)
            label = label_error(msg)

            # FAILED일 때만 출력되므로 저장만
            res.attempt_fail_logs.append(
                f"{attempt+1}회차 실패 | stage={stage} | label={label} | reason={reason} | detail={msg[-220:]}"
            )

            final_stage, final_label, final_reason, final_detail = stage, label, reason, msg[-900:]

            if attempt < MAX_RETRIES:
                time.sleep(3)

    res.status = "FAILED"
    res.failed_stage = final_stage or "UNKNOWN"
    res.fail_label = final_label or "UNKNOWN"
    res.reason = final_reason or "알 수 없는 실패"
    res.error_detail = final_detail or "상세 오류 없음"
    return res


# =========================
# 메인
# =========================

def main():
    if os.path.exists(WORKDIR):
        shutil.rmtree(WORKDIR, ignore_errors=True)
    os.makedirs(AUDIODIR, exist_ok=True)
    os.makedirs(SEGDIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    uc_cache = load_uc_cache()
    model = whisper.load_model(WHISPER_MODEL)

    results: List[ChannelResult] = []
    for name, url in CHANNELS.items():
        r = process_channel(name, url, uc_cache, model)
        results.append(r)

    save_uc_cache(uc_cache)

    # uc_ids.txt 생성
    uc_lines: List[str] = []
    uc_lines.append(f"# UC IDs - {kst_now_str()}")
    uc_lines.append("# format: name<TAB>uc_id<TAB>videos_url")
    for r in results:
        if r.uc_id:
            uc_lines.append(f"{r.channel}\t{r.uc_id}\t{r.resolved_videos_url}")
        else:
            uc_lines.append(f"{r.channel}\t<UC_ID_NOT_AVAILABLE>\t{r.channel_url}")
    with open(UC_IDS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(uc_lines).rstrip() + "\n")

    # report.txt 생성
    lines: List[str] = []
    lines.append("[미국 주식 시황 리포트 - 자동 생성]")
    lines.append(f"생성 시각: {kst_now_str()}")
    lines.append(f"기준: 최근 {RECENT_HOURS}시간, 종료된 라이브 포함(was_live), 진행 중 라이브 제외(is_live)")
    lines.append(f"세그먼트 분할: {SEGMENT_SECONDS//60}분 단위")
    lines.append("")

    lines.append("■ 채널별 텍스트화 결과")
    for r in results:
        lines.append(f"- {r.channel}")

        if r.status == "SUCCESS":
            lines.append("  상태: SUCCESS")
            lines.append(f"  시도횟수: {r.attempts}")
            lines.append(f"  UC ID: {r.uc_id}")
            lines.append(f"  콘텐츠: {r.video_title or '제목 없음'}")
            lines.append(f"  URL: {r.video_url}")
            if r.timestamp:
                dt = datetime.fromtimestamp(r.timestamp, tz=timezone.utc) + timedelta(hours=9)
                lines.append(f"  게시/종료 시각(추정): {dt.strftime('%Y-%m-%d %H:%M (KST)')}")
            lines.append(f"  live_status: {r.live_status}")
            lines.append(f"  텍스트화: 성공 (문자수 {r.transcript_chars})")

        elif r.status == "NO_VIDEO":
            lines.append("  상태: NO_VIDEO")
            lines.append(f"  UC ID: {r.uc_id}")
            lines.append(f"  사유: {r.reason}")

        else:
            lines.append("  상태: FAILED")
            lines.append(f"  시도횟수: {r.attempts}")
            lines.append(f"  UC ID: {r.uc_id or 'N/A'}")
            lines.append(f"  실패단계: {r.failed_stage}")
            lines.append(f"  원인라벨: {r.fail_label}")
            lines.append(f"  사유: {r.reason}")
            if r.video_url:
                lines.append(f"  URL: {r.video_url}")
            if r.error_detail:
                lines.append(f"  상세: {r.error_detail}")
            if r.attempt_fail_logs:
                lines.append("  시도별 실패 기록:")
                for logline in r.attempt_fail_logs:
                    lines.append(f"   - {logline}")

        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("[UC ID 모음 파일]")
    lines.append(f"- {UC_IDS_FILE} 에 채널명-UCID-영상탭 URL이 저장됩니다.")
    lines.append("")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print("report.txt / uc_ids.txt 생성 완료")


if __name__ == "__main__":
    main()

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

RECENT_HOURS = 24                 # upload_date가 일단위라 24h 권장
SEGMENT_SECONDS = 15 * 60         # 15분 분할
MAX_RETRIES = 2                   # "무조건 재다운 2회" => 총 3회
WHISPER_MODEL = "base"            # GitHub Actions CPU 기준

WORKDIR = "_work"
AUDIODIR = os.path.join(WORKDIR, "audio")
SEGDIR = os.path.join(WORKDIR, "segments")
CACHE_DIR = os.path.join(WORKDIR, "cache")
UC_CACHE_PATH = os.path.join(CACHE_DIR, "uc_cache.json")

# ✅ 이제 UC ID를 직접 넣지 않아도 됩니다. handle/URL만 넣으면 됩니다.
CHANNELS = {
    "Bloomberg": "https://www.youtube.com/@BloombergTV",
    "Meet Kevin": "https://www.youtube.com/@MeetKevin",
    "오선의 미국 증시": "https://www.youtube.com/@futuresnow",
    "설명왕 테이버": "https://www.youtube.com/@taver",
    "내일은 투자왕 - 김단테": "https://www.youtube.com/@kimdante",
    "소수몽키": "https://www.youtube.com/@sosumonkey",
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


def parse_upload_date_yyyymmdd(s: str) -> Optional[datetime]:
    if not s or not re.fullmatch(r"\d{8}", s):
        return None
    return datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)


def within_recent_window(upload_dt: Optional[datetime], hours: int) -> bool:
    if upload_dt is None:
        return False
    return (utc_now() - upload_dt) <= timedelta(hours=hours)


def label_error(stderr_or_msg: str) -> str:
    s = (stderr_or_msg or "").lower()
    if "http error 403" in s or "403 forbidden" in s:
        return "HTTP_403"
    if "http error 401" in s or "401" in s:
        return "HTTP_401"
    if "http error 429" in s or "too many requests" in s or "429" in s:
        return "HTTP_429"
    if "http error 404" in s or "requested entity was not found" in s or "404" in s:
        return "HTTP_404"
    if "not available in your country" in s or "geoblock" in s:
        return "GEO_BLOCK"
    if "sign in to confirm your age" in s or "age-restricted" in s:
        return "AGE_RESTRICTED"
    if "private video" in s or "this video is private" in s:
        return "PRIVATE_VIDEO"
    if "premiere" in s and ("not started" in s or "will begin" in s):
        return "PREMIERE_NOT_STARTED"
    if "live" in s and ("is currently live" in s or "livestream" in s or "live event" in s):
        return "LIVE"
    if "no video formats found" in s:
        return "NO_FORMAT"
    if "requested format is not available" in s:
        return "FORMAT_UNAVAILABLE"
    if "unable to download webpage" in s or "failed to download" in s:
        return "DOWNLOAD_WEBPAGE_FAIL"
    if "could not find ffmpeg" in s:
        return "FFMPEG_MISSING"
    if "ffmpeg" in s and ("error" in s or "invalid" in s or "failed" in s):
        return "FFMPEG_FAIL"
    if "timeout" in s or "timed out" in s:
        return "TIMEOUT"
    if "whisper 결과 텍스트가 비어" in s or ("empty" in s and "text" in s):
        return "WHISPER_EMPTY"
    return "UNKNOWN"


def stage_reason_from_exception(msg: str) -> Tuple[str, str]:
    m = msg or ""
    if "yt-dlp 채널 조회 실패" in m or "채널 조회 실패" in m or "channel_id 추출 실패" in m:
        return "FETCH", "채널 메타/ID 조회 실패"
    if "오디오 다운로드 실패" in m or "오디오 파일이 생성되지 않음" in m:
        return "DOWNLOAD", "오디오 다운로드 실패(차단/접근제한/포맷)"
    if "ffmpeg 정규화 실패" in m:
        return "DOWNLOAD", "오디오 정규화 실패(ffmpeg/코덱)"
    if "ffmpeg 분할 실패" in m:
        return "SPLIT", "오디오 분할 실패(ffmpeg segment)"
    if "Whisper 결과 텍스트가 비어" in m:
        return "TRANSCRIBE", "Whisper 결과 텍스트 비어 있음"
    return "TRANSCRIBE", "처리 중 알 수 없는 오류"


# =========================
# UC ID 자동 해석 + 캐시
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


def resolve_uc_id(channel_url: str, cache: Dict[str, str]) -> str:
    """
    어떤 형태(@handle / c / channel / m.youtube)로 들어와도
    yt-dlp를 이용해 channel_id(UC...)를 추출합니다.
    """
    # 모바일 링크면 정규화
    channel_url = channel_url.replace("m.youtube.com", "www.youtube.com").rstrip("/")

    if channel_url in cache:
        return cache[channel_url]

    # 채널 영상 목록에서 1개만 뽑아 channel_id를 얻는다(가장 단단한 방법)
    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--flat-playlist",
        "--playlist-end", "1",
        f"{channel_url}/videos"
    ]
    rc, out, err = run(cmd, timeout=180)
    if rc != 0:
        raise RuntimeError(f"채널 조회 실패({label_error(err)}): {err[-500:]}")

    data = json.loads(out)
    channel_id = data.get("channel_id") or data.get("uploader_id")
    if not channel_id or not channel_id.startswith("UC"):
        raise RuntimeError("channel_id 추출 실패")

    cache[channel_url] = channel_id
    return channel_id


def make_channel_videos_url(uc_id: str) -> str:
    return f"https://www.youtube.com/channel/{uc_id}/videos"


# =========================
# 최신 영상 선택 (UC 기반 videos)
# =========================

def pick_latest_video(channel_videos_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    cmd = ["yt-dlp", "--flat-playlist", "--dump-single-json", channel_videos_url]
    rc, out, err = run(cmd, timeout=180)
    if rc != 0:
        raise RuntimeError(f"yt-dlp 채널 조회 실패({label_error(err)}): {err[-500:]}")

    data = json.loads(out)
    entries = data.get("entries") or []

    for e in entries[:30]:
        vid = e.get("id")
        title = e.get("title") or ""
        upload_date = e.get("upload_date")  # YYYYMMDD
        if not vid:
            continue
        upload_dt = parse_upload_date_yyyymmdd(upload_date or "")
        if upload_dt and within_recent_window(upload_dt, RECENT_HOURS):
            return f"https://www.youtube.com/watch?v={vid}", title, upload_date

    return None, None, None


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
        raise RuntimeError(f"오디오 다운로드 실패({label_error(err)}): {err[-500:]}")

    for fn in os.listdir(AUDIODIR):
        if fn.startswith(out_base + "."):
            return os.path.join(AUDIODIR, fn)

    raise RuntimeError("오디오 파일이 생성되지 않음(FILE_NOT_FOUND)")


def normalize_to_wav_mono16k(input_audio: str, out_wav: str) -> None:
    cmd = ["ffmpeg", "-y", "-i", input_audio, "-ar", "16000", "-ac", "1", "-vn", out_wav]
    rc, out, err = run(cmd, timeout=900)
    if rc != 0:
        raise RuntimeError(f"ffmpeg 정규화 실패({label_error(err)}): {err[-500:]}")


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
        raise RuntimeError(f"ffmpeg 분할 실패({label_error(err)}): {err[-500:]}")

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

    channel_url: Optional[str] = None
    uc_id: Optional[str] = None
    resolved_videos_url: Optional[str] = None

    video_url: Optional[str] = None
    video_title: Optional[str] = None
    upload_date: Optional[str] = None

    attempts: int = 0

    failed_stage: Optional[str] = None
    fail_label: Optional[str] = None
    reason: Optional[str] = None
    error_detail: Optional[str] = None

    attempt_fail_logs: List[str] = field(default_factory=list)
    transcript_chars: int = 0


# =========================
# 채널 처리(UC 자동해석 + 무조건 재다운 2회 + FAILED만 로그)
# =========================

def process_channel(name: str, url: str, cache: Dict[str, str], whisper_model) -> ChannelResult:
    res = ChannelResult(channel=name, status="FAILED", channel_url=url)

    # 1) UC ID 해석
    try:
        uc_id = resolve_uc_id(url, cache)
        res.uc_id = uc_id
        res.resolved_videos_url = make_channel_videos_url(uc_id)
    except Exception as e:
        msg = str(e)
        res.status = "FAILED"
        res.failed_stage = "FETCH"
        res.fail_label = label_error(msg)
        res.reason = "UC ID(채널 고유 ID) 해석 실패"
        res.error_detail = msg[-800:]
        return res

    # 2) 최신 영상 선택
    try:
        vurl, title, udate = pick_latest_video(res.resolved_videos_url)
        if not vurl:
            res.status = "NO_VIDEO"
            res.reason = f"최근 {RECENT_HOURS}시간 내 업로드 영상 없음"
            return res
        res.video_url = vurl
        res.video_title = title
        res.upload_date = udate
    except Exception as e:
        msg = str(e)
        res.status = "FAILED"
        res.failed_stage = "FETCH"
        res.fail_label = label_error(msg)
        res.reason = "채널에서 최신 영상 조회 실패"
        res.error_detail = msg[-800:]
        return res

    # 3) 오디오→분할→Whisper (무조건 3회 시도, 매번 재다운)
    base = safe_filename(f"{name}_{res.upload_date or 'nodate'}")

    final_stage = None
    final_label = None
    final_reason = None
    final_detail = None

    for attempt in range(MAX_RETRIES + 1):
        res.attempts = attempt + 1
        try:
            os.makedirs(AUDIODIR, exist_ok=True)
            os.makedirs(SEGDIR, exist_ok=True)

            # 매 시도마다 잔여물 삭제 => "무조건 재다운"
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

            # 성공 시: 실패기록은 report에 남기지 않는다
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

            # 시도별 실패는 저장만(FAILED일 때만 출력)
            res.attempt_fail_logs.append(
                f"{attempt+1}회차 실패 | stage={stage} | label={label} | reason={reason} | detail={msg[-220:]}"
            )

            final_stage = stage
            final_label = label
            final_reason = reason
            final_detail = msg[-800:]

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
    # 작업 폴더 초기화
    if os.path.exists(WORKDIR):
        shutil.rmtree(WORKDIR, ignore_errors=True)
    os.makedirs(AUDIODIR, exist_ok=True)
    os.makedirs(SEGDIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    # UC 캐시 로드
    uc_cache = load_uc_cache()

    # Whisper 모델 로드
    model = whisper.load_model(WHISPER_MODEL)

    results: List[ChannelResult] = []

    for name, url in CHANNELS.items():
        r = process_channel(name, url, uc_cache, model)
        results.append(r)

    # UC 캐시 저장(런타임 캐시)
    save_uc_cache(uc_cache)

    # 1) uc_ids.txt 생성 (이름 빠진 UC ID 모음)
    #   - 성공/실패 상관없이 UC 해석이 된 채널은 모두 기록
    uc_lines: List[str] = []
    uc_lines.append(f"# UC IDs (auto-collected) - {kst_now_str()}")
    uc_lines.append("# format: name<TAB>uc_id<TAB>resolved_videos_url")
    for r in results:
        if r.uc_id:
            uc_lines.append(f"{r.channel}\t{r.uc_id}\t{r.resolved_videos_url}")
        else:
            # UC 해석 실패도 남겨서 나중에 추적 가능
            uc_lines.append(f"{r.channel}\t<UC_ID_NOT_RESOLVED>\t{r.channel_url}")
    with open(UC_IDS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(uc_lines).rstrip() + "\n")

    # 2) report.txt 생성
    lines: List[str] = []
    lines.append("[미국 주식 시황 리포트 - 자동 생성]")
    lines.append(f"생성 시각: {kst_now_str()}")
    lines.append(f"최근 업로드 필터: {RECENT_HOURS}시간")
    lines.append(f"세그먼트 분할: {SEGMENT_SECONDS//60}분 단위")
    lines.append("")

    lines.append("■ 채널별 텍스트화 결과")
    for r in results:
        lines.append(f"- {r.channel}")

        if r.status == "SUCCESS":
            lines.append("  상태: SUCCESS")
            lines.append(f"  시도횟수: {r.attempts}")
            lines.append(f"  UC ID: {r.uc_id}")
            lines.append(f"  영상: {r.video_title or '제목 없음'}")
            lines.append(f"  URL: {r.video_url}")
            lines.append(f"  업로드일: {r.upload_date or '알 수 없음'}")
            lines.append(f"  텍스트화: 성공 (문자수 {r.transcript_chars})")

        elif r.status == "NO_VIDEO":
            lines.append("  상태: NO_VIDEO")
            lines.append(f"  UC ID: {r.uc_id}")
            lines.append(f"  사유: {r.reason}")

        else:
            # FAILED일 때만 실패기록 출력
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
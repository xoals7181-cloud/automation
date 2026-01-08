import os
import re
import json
import time
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import whisper  # pip install openai-whisper

# =========================
# 설정
# =========================

REPORT_FILE = "report.txt"

# YouTube upload_date는 보통 YYYYMMDD(일 단위)라 12시간 필터가 정확하지 않습니다.
# 운영 안정성을 위해 24시간으로 고정 권장.
RECENT_HOURS = 24

# 세그먼트 분할(초) - 15분
SEGMENT_SECONDS = 15 * 60

# "무조건 재다운로드 + 재시도": 재시도 2회 = 총 3회
MAX_RETRIES = 2

# Whisper 모델(깃헙 액션 CPU 기준 base 권장, small은 더 정확하지만 느림)
WHISPER_MODEL = "base"

# 반드시 /videos로 끝나게
CHANNELS = {
    "Bloomberg": "https://www.youtube.com/@BloombergTV/videos",
    "Meet Kevin": "https://www.youtube.com/@MeetKevin/videos",
    # 아래는 예시(실제 핸들이 다르면 교체)
    "오선의 미국 증시": "https://www.youtube.com/@osunstock/videos",
    "설명왕 테이버": "https://www.youtube.com/@taver/videos",
    "뉴욕주민": "https://www.youtube.com/@nyresident/videos",
}

WORKDIR = "_work"
AUDIODIR = os.path.join(WORKDIR, "audio")
SEGDIR = os.path.join(WORKDIR, "segments")


# =========================
# 유틸
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


def parse_upload_date_yyyymmdd(s: str) -> Optional[datetime]:
    if not s or not re.fullmatch(r"\d{8}", s):
        return None
    return datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)


def within_recent_window(upload_dt: Optional[datetime], hours: int) -> bool:
    if upload_dt is None:
        return False
    return (utc_now() - upload_dt) <= timedelta(hours=hours)


def kst_now_str() -> str:
    return (utc_now() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M (KST)")


# =========================
# 결과 구조 (필수)
# =========================

@dataclass
class ChannelResult:
    channel: str
    status: str  # SUCCESS / NO_VIDEO / FAILED

    video_url: Optional[str] = None
    video_title: Optional[str] = None
    upload_date: Optional[str] = None

    attempts: int = 0

    # 실패 분석 (FAILED일 때만 의미 있게 출력)
    failed_stage: Optional[str] = None  # FETCH / DOWNLOAD / SPLIT / TRANSCRIBE
    fail_label: Optional[str] = None    # 정확한 원인 라벨
    reason: Optional[str] = None        # 사람이 읽기 쉬운 요약
    error_detail: Optional[str] = None  # 마지막 에러/스택/스텟더 요약

    # 디버깅용: 시도별 실패 기록(FAILED일 때만 리포트에 출력)
    attempt_fail_logs: List[str] = field(default_factory=list)

    # 산출물 요약
    transcript_chars: int = 0


# =========================
# 원인 라벨링
# =========================

def label_error(stderr_or_msg: str) -> str:
    s = (stderr_or_msg or "").lower()

    # HTTP/접근 계열
    if "http error 403" in s or "403 forbidden" in s:
        return "HTTP_403"
    if "http error 401" in s or "401" in s:
        return "HTTP_401"
    if "http error 429" in s or "too many requests" in s or "429" in s:
        return "HTTP_429"
    if "http error 404" in s or "404" in s:
        return "HTTP_404"
    if "geoblock" in s or "not available in your country" in s or "country" in s:
        return "GEO_BLOCK"
    if "sign in to confirm your age" in s or "age-restricted" in s:
        return "AGE_RESTRICTED"
    if "private video" in s or "this video is private" in s:
        return "PRIVATE_VIDEO"
    if "premiere" in s and ("not started" in s or "will begin" in s):
        return "PREMIERE_NOT_STARTED"
    if "live" in s and ("is currently live" in s or "this live event" in s or "livestream" in s):
        return "LIVE"

    # 다운로드/포맷 계열
    if "no video formats found" in s:
        return "NO_FORMAT"
    if "requested format is not available" in s:
        return "FORMAT_UNAVAILABLE"
    if "unable to download webpage" in s or "failed to download" in s:
        return "DOWNLOAD_WEBPAGE_FAIL"
    if "no such file or directory" in s:
        return "FILE_NOT_FOUND"

    # ffmpeg 계열
    if "ffmpeg" in s and ("error" in s or "invalid" in s or "failed" in s):
        return "FFMPEG_FAIL"
    if "could not find ffmpeg" in s:
        return "FFMPEG_MISSING"

    # whisper/stt 계열
    if "whisper 결과 텍스트가 비어" in s or "empty" in s and "text" in s:
        return "WHISPER_EMPTY"
    if "cuda" in s and "not available" in s:
        return "CUDA_NOT_AVAILABLE"
    if "timeout" in s or "timed out" in s:
        return "TIMEOUT"

    return "UNKNOWN"


def stage_reason_from_exception(msg: str) -> Tuple[str, str]:
    """Exception 메시지로 stage, reason을 추정"""
    m = msg or ""
    if "yt-dlp 채널 조회 실패" in m or "채널 조회 실패" in m:
        return "FETCH", "채널에서 최신 영상 목록 조회 실패"
    if "오디오 다운로드 실패" in m or "오디오 파일이 생성되지 않음" in m:
        return "DOWNLOAD", "오디오 다운로드 실패(유튜브 차단/접근 제한/포맷 이슈)"
    if "ffmpeg 정규화 실패" in m:
        return "DOWNLOAD", "오디오 정규화 실패(ffmpeg 코덱/파일 손상)"
    if "ffmpeg 분할 실패" in m:
        return "SPLIT", "오디오 분할 실패(ffmpeg segment 처리 오류)"
    if "Whisper 결과 텍스트가 비어" in m:
        return "TRANSCRIBE", "음성→텍스트 결과가 비어 있음(무음/차단/디코딩)"
    return "TRANSCRIBE", "처리 중 알 수 없는 오류"


# =========================
# 1) 채널에서 최신 영상 찾기 (yt-dlp flat json)
# =========================

def pick_latest_video(channel_videos_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    cmd = ["yt-dlp", "--flat-playlist", "--dump-single-json", channel_videos_url]
    rc, out, err = run(cmd, timeout=180)
    if rc != 0:
        raise RuntimeError(f"yt-dlp 채널 조회 실패: {err[-500:]}")

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
# 2) 오디오 다운로드 + 정규화
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
        # yt-dlp stderr 기반 라벨링
        label = label_error(err)
        raise RuntimeError(f"오디오 다운로드 실패 ({label}): {err[-500:]}")

    # 생성된 파일 찾기
    for fn in os.listdir(AUDIODIR):
        if fn.startswith(out_base + "."):
            return os.path.join(AUDIODIR, fn)

    raise RuntimeError("오디오 파일이 생성되지 않음(FILE_NOT_FOUND)")


def normalize_to_wav_mono16k(input_audio: str, out_wav: str) -> None:
    cmd = ["ffmpeg", "-y", "-i", input_audio, "-ar", "16000", "-ac", "1", "-vn", out_wav]
    rc, out, err = run(cmd, timeout=900)
    if rc != 0:
        raise RuntimeError(f"ffmpeg 정규화 실패 (FFMPEG_FAIL): {err[-500:]}")


# =========================
# 3) 길이 분할 (세그먼트)
# =========================

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
        raise RuntimeError(f"ffmpeg 분할 실패 (FFMPEG_FAIL): {err[-500:]}")

    segs = sorted(
        os.path.join(SEGDIR, f) for f in os.listdir(SEGDIR)
        if f.startswith(os.path.basename(prefix) + "_") and f.endswith(".wav")
    )
    return segs if segs else [input_wav]


# =========================
# 4) Whisper 텍스트화
# =========================

def transcribe_segments(model, segments: List[str]) -> str:
    texts: List[str] = []
    for seg in segments:
        result = model.transcribe(seg, fp16=False, temperature=0)
        text = (result.get("text") or "").strip()
        if text:
            texts.append(text)
    return "\n".join(texts).strip()


# =========================
# 5) 채널 처리 (무조건 재다운 2회 + 최종 실패만 기록)
# =========================

def process_channel(channel: str, channel_url: str, whisper_model) -> ChannelResult:
    res = ChannelResult(channel=channel, status="FAILED")

    # (A) 최신 영상 선택
    try:
        video_url, title, upload_date = pick_latest_video(channel_url)
        if not video_url:
            res.status = "NO_VIDEO"
            res.reason = f"최근 {RECENT_HOURS}시간 내 업로드 영상 없음"
            return res
        res.video_url = video_url
        res.video_title = title
        res.upload_date = upload_date
    except Exception as e:
        msg = str(e)
        res.status = "FAILED"
        res.failed_stage = "FETCH"
        res.fail_label = label_error(msg)
        res.reason = "채널에서 최신 영상 조회 실패"
        res.error_detail = msg[-700:]
        return res

    base = safe_filename(f"{channel}_{res.upload_date or 'nodate'}")

    # 최종 실패 정보(3회 모두 실패했을 때만 기록)
    final_stage = None
    final_label = None
    final_reason = None
    final_detail = None

    # ✅ 무조건 3회(최초 1 + 재다운 2)
    for attempt in range(MAX_RETRIES + 1):
        res.attempts = attempt + 1

        try:
            os.makedirs(AUDIODIR, exist_ok=True)
            os.makedirs(SEGDIR, exist_ok=True)

            # ✅ 매 시도마다 잔여물 전부 삭제 (무조건 재다운 보장)
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

            # 1) 오디오 다운로드 (항상 새로)
            audio_path = download_audio(res.video_url, base)

            # 2) 정규화
            wav_path = os.path.join(AUDIODIR, f"{base}_clean.wav")
            normalize_to_wav_mono16k(audio_path, wav_path)

            # 3) 분할
            segs = split_wav(wav_path, base)

            # 4) Whisper
            transcript = transcribe_segments(whisper_model, segs)
            if not transcript:
                raise RuntimeError("Whisper 결과 텍스트가 비어 있음(WHISPER_EMPTY)")

            # ✅ 성공: FAILED 기록은 남기지 않는다(요청사항)
            res.status = "SUCCESS"
            res.transcript_chars = len(transcript)
            res.failed_stage = None
            res.fail_label = None
            res.error_detail = None
            res.reason = f"성공 (총 {res.attempts}회 시도)" if res.attempts > 1 else "성공"
            return res

        except Exception as e:
            msg = str(e)
            stage, reason = stage_reason_from_exception(msg)
            label = label_error(msg)

            # 시도별 로그는 'FAILED일 때만 출력'하므로 여기선 저장만
            res.attempt_fail_logs.append(
                f"{attempt+1}회차 실패 | stage={stage} | label={label} | reason={reason} | detail={msg[-220:]}"
            )

            final_stage = stage
            final_label = label
            final_reason = reason
            final_detail = msg[-800:]

            # ✅ 조건 없이 재시도
            if attempt < MAX_RETRIES:
                time.sleep(3)

    # ✅ 3회 모두 실패: 여기서만 FAILED 기록 확정
    res.status = "FAILED"
    res.failed_stage = final_stage or "UNKNOWN"
    res.fail_label = final_label or "UNKNOWN"
    res.reason = final_reason or "알 수 없는 실패"
    res.error_detail = final_detail or "상세 오류 없음"
    return res


# =========================
# 메인: report.txt 작성 (FAILED일 때만 실패 기록 출력)
# =========================

def main():
    # 작업 폴더 초기화
    if os.path.exists(WORKDIR):
        shutil.rmtree(WORKDIR, ignore_errors=True)
    os.makedirs(AUDIODIR, exist_ok=True)
    os.makedirs(SEGDIR, exist_ok=True)

    model = whisper.load_model(WHISPER_MODEL)

    lines: List[str] = []
    lines.append("[미국 주식 시황 리포트 - 자동 생성]")
    lines.append(f"생성 시각: {kst_now_str()}")
    lines.append(f"최근 업로드 필터: {RECENT_HOURS}시간")
    lines.append(f"세그먼트 분할: {SEGMENT_SECONDS//60}분 단위")
    lines.append(f"재시도: 무조건 재다운 2회 (총 3회)")
    lines.append("")

    lines.append("■ 채널별 텍스트화 결과")
    results: List[ChannelResult] = []

    for ch, url in CHANNELS.items():
        r = process_channel(ch, url, model)
        results.append(r)

    for r in results:
        lines.append(f"- {r.channel}")

        if r.status == "SUCCESS":
            lines.append("  상태: SUCCESS")
            lines.append(f"  시도횟수: {r.attempts}")
            lines.append(f"  영상: {r.video_title or '제목 없음'}")
            lines.append(f"  URL: {r.video_url}")
            lines.append(f"  업로드일: {r.upload_date or '알 수 없음'}")
            lines.append(f"  텍스트화: 성공 (문자수 {r.transcript_chars})")

        elif r.status == "NO_VIDEO":
            lines.append("  상태: NO_VIDEO")
            lines.append(f"  사유: {r.reason}")

        else:
            # ✅ FAILED일 때만 실패 기록 출력
            lines.append("  상태: FAILED")
            lines.append(f"  시도횟수: {r.attempts}")
            lines.append(f"  실패단계: {r.failed_stage}")
            lines.append(f"  원인라벨: {r.fail_label}")
            lines.append(f"  사유: {r.reason}")
            if r.video_url:
                lines.append(f"  URL: {r.video_url}")
            if r.error_detail:
                lines.append(f"  상세: {r.error_detail}")

            # (옵션) 시도별 실패 기록도 FAILED일 때만 표시
            if r.attempt_fail_logs:
                lines.append("  시도별 실패 기록:")
                for logline in r.attempt_fail_logs:
                    lines.append(f"   - {logline}")

        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("[기타 사항]")
    lines.append("- 본 버전은 '오디오 다운로드 → 분할 → Whisper 텍스트화' 성공률을 최대화하고, FAILED 시 원인라벨을 남깁니다.")
    lines.append("- 다음 단계에서 SUCCESS 채널의 텍스트를 이용해 리포트 템플릿(핵심이슈/전문가/대중심리/메타판단)을 자동 생성합니다.")
    lines.append("")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print("report.txt 생성 완료")


if __name__ == "__main__":
    main()
import os
import re
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

# =========================
# 설정
# =========================

REPORT_FILE = "report.txt"
RECENT_HOURS = 12

# ✅ UC ID 기반(가장 안정)
CHANNELS = {
    "Bloomberg": "UCIALMKvObZNtJ6AmdCLP7Lg",
    "Meet Kevin": "UCUvvj5lwue7PspotMDjk5UA",
    "오선의 미국 증시": "UC_JJ_NhRqPKcIOj5Ko3W_3w",
    "설명왕 테이버": "UCOio3vyYLWiKlHSYRKW-9UA",
    "내일은 투자왕 - 김단테": "UCKTMvIu9a4VGSrpWy-8bUrQ",
    "소수몽키": "UCC3yfxS5qC6PCwDzetUuEWg",
}

# =========================
# 유틸
# =========================

def kst_now_str() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M (KST)")

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    return s

# =========================
# YouTube Data API
# =========================

def youtube_search_latest(channel_id: str, published_after: datetime, api_key: str, max_results: int = 5) -> List[dict]:
    """
    최근 12시간 내 업로드된 영상(및 종료된 라이브 포함 가능) 후보를 조회
    """
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "channelId": channel_id,
        "order": "date",
        "type": "video",
        "maxResults": max_results,
        "publishedAfter": iso_utc(published_after),
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"YouTube API search error {r.status_code}: {r.text[:300]}")
    return r.json().get("items", [])

def youtube_videos_details(video_ids: List[str], api_key: str) -> Dict[str, dict]:
    """
    영상 상세(라이브 여부/상태 포함) 조회
    """
    if not video_ids:
        return {}
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,contentDetails,liveStreamingDetails",
        "id": ",".join(video_ids),
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"YouTube API videos error {r.status_code}: {r.text[:300]}")
    items = r.json().get("items", [])
    return {it["id"]: it for it in items}

def is_live_ongoing(video_detail: dict) -> bool:
    lsd = video_detail.get("liveStreamingDetails") or {}
    # 진행 중 라이브는 actualEndTime이 없음 + actualStartTime만 있는 케이스가 많음
    return ("actualStartTime" in lsd) and ("actualEndTime" not in lsd)

# =========================
# Transcript
# =========================

def fetch_transcript_text(video_id: str, prefer_langs=("ko", "en")) -> str:
    """
    자막이 있으면 가져오고, 없으면 예외를 던진다.
    """
    # 가능한 transcript 목록 조회
    transcripts = YouTubeTranscriptApi.list_transcripts(video_id)

    # 선호 언어 우선
    for lang in prefer_langs:
        try:
            t = transcripts.find_transcript([lang])
            parts = t.fetch()
            return clean_text(" ".join(p.get("text", "") for p in parts))
        except:
            pass

    # 자동번역 자막도 시도
    for lang in prefer_langs:
        try:
            t = transcripts.find_transcript([lang])
            # 위에서 실패하면 여기까지 보통 안 오지만 형태 유지
        except:
            pass

    # 아무것도 안 되면 NoTranscriptFound로 처리
    raise NoTranscriptFound(video_id)

# =========================
# 결과 구조
# =========================

@dataclass
class ChannelResult:
    channel: str
    status: str  # SUCCESS / NO_VIDEO / NO_TRANSCRIPT / API_ERROR
    video_id: Optional[str] = None
    title: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[str] = None
    note: Optional[str] = None
    transcript_chars: int = 0

# =========================
# 메인 처리
# =========================

def process_channel(name: str, channel_id: str, api_key: str) -> ChannelResult:
    now = datetime.now(timezone.utc)
    published_after = now - timedelta(hours=RECENT_HOURS)

    try:
        items = youtube_search_latest(channel_id, published_after, api_key, max_results=5)
        if not items:
            return ChannelResult(channel=name, status="NO_VIDEO", note=f"최근 {RECENT_HOURS}시간 내 영상 없음")

        video_ids = []
        basic = {}
        for it in items:
            vid = it.get("id", {}).get("videoId")
            if not vid:
                continue
            video_ids.append(vid)
            basic[vid] = it.get("snippet", {})

        details = youtube_videos_details(video_ids, api_key)

        # 종료된 라이브 포함, 진행 중 라이브는 제외
        chosen_id = None
        for vid in video_ids:
            d = details.get(vid, {})
            if not d:
                continue
            if is_live_ongoing(d):
                continue
            chosen_id = vid
            break

        if not chosen_id:
            return ChannelResult(channel=name, status="NO_VIDEO", note=f"최근 {RECENT_HOURS}시간 내 종료된 콘텐츠 없음(진행중 라이브 제외)")

        sn = details[chosen_id].get("snippet", {})
        title = sn.get("title")
        published_at = sn.get("publishedAt")
        url = f"https://www.youtube.com/watch?v={chosen_id}"

        # transcript 시도
        try:
            text = fetch_transcript_text(chosen_id)
            return ChannelResult(
                channel=name,
                status="SUCCESS",
                video_id=chosen_id,
                title=title,
                url=url,
                published_at=published_at,
                transcript_chars=len(text),
                note="자막 기반 텍스트 추출 성공"
            )
        except (TranscriptsDisabled, NoTranscriptFound):
            return ChannelResult(
                channel=name,
                status="NO_TRANSCRIPT",
                video_id=chosen_id,
                title=title,
                url=url,
                published_at=published_at,
                note="자막 없음/비활성화(Transcript 불가)"
            )

    except Exception as e:
        return ChannelResult(channel=name, status="API_ERROR", note=str(e)[:300])

def main():
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY가 설정되지 않았습니다(GitHub Secrets 확인).")

    results: List[ChannelResult] = []
    for name, cid in CHANNELS.items():
        results.append(process_channel(name, cid, api_key))

    lines: List[str] = []
    lines.append("[미국 주식 시황 리포트 - 안정형]")
    lines.append(f"생성 시각: {kst_now_str()}")
    lines.append(f"최근 필터: {RECENT_HOURS}시간 (종료된 라이브 포함, 진행중 라이브 제외)")
    lines.append("")
    lines.append("■ 채널별 결과")

    for r in results:
        lines.append(f"- {r.channel}")
        lines.append(f"  상태: {r.status}")
        if r.title:
            lines.append(f"  제목: {r.title}")
        if r.url:
            lines.append(f"  URL: {r.url}")
        if r.published_at:
            lines.append(f"  게시: {r.published_at}")
        if r.status == "SUCCESS":
            lines.append(f"  텍스트화: 성공(문자수 {r.transcript_chars})")
        if r.note:
            lines.append(f"  비고: {r.note}")
        lines.append("")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print("report.txt 생성 완료")

if __name__ == "__main__":
    main()

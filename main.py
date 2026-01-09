import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from youtube_transcript_api import YouTubeTranscriptApi

REPORT_FILE = "report.txt"

RECENT_HOURS = 12
SEARCH_HOURS = 48

CHANNELS = {
    "Bloomberg": "UCIALMKvObZNtJ6AmdCLP7Lg",
    "Meet Kevin": "UCUvvj5lwue7PspotMDjk5UA",
    "오선의 미국 증시": "UC_JJ_NhRqPKcIOj5Ko3W_3w",
    "설명왕 테이버": "UCOio3vyYLWiKlHSYRKW-9UA",
    "내일은 투자왕 - 김단테": "UCKTMvIu9a4VGSrpWy-8bUrQ",
    "소수몽키": "UCC3yfxS5qC6PCwDzetUuEWg",
}

def kst_now_str() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M (KST)")

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

# ---------- YouTube Data API ----------

def youtube_search_latest(channel_id: str, published_after: datetime, api_key: str, max_results: int = 10) -> List[dict]:
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
    if not video_ids:
        return {}
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,liveStreamingDetails,contentDetails",
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
    return ("actualStartTime" in lsd) and ("actualEndTime" not in lsd)

def effective_time_for_filter(video_detail: dict) -> Optional[datetime]:
    """
    12시간 필터 기준 시간:
    - 종료된 라이브: actualEndTime 우선
    - 그 외: publishedAt
    """
    lsd = video_detail.get("liveStreamingDetails") or {}
    end_time = parse_dt(lsd.get("actualEndTime"))
    if end_time:
        return end_time
    published_at = video_detail.get("snippet", {}).get("publishedAt")
    return parse_dt(published_at)

# ---------- Transcript (버전 차이 안전) ----------

def fetch_transcript_text(video_id: str, prefer_langs=("ko", "en")) -> Optional[str]:
    """
    자막 있으면 텍스트, 없으면 None.
    (NoTranscriptFound 같은 예외 생성/시그니처 차이를 완전히 회피)
    """
    try:
        if hasattr(YouTubeTranscriptApi, "list_transcripts"):
            transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
            for lang in prefer_langs:
                try:
                    t = transcripts.find_transcript([lang])
                    parts = t.fetch()
                    text = clean_text(" ".join(p.get("text", "") for p in parts))
                    return text if text else None
                except Exception:
                    pass
            return None

        for lang in prefer_langs:
            try:
                parts = YouTubeTranscriptApi.get_transcript(video_id, languages=[lang])
                text = clean_text(" ".join(p.get("text", "") for p in parts))
                return text if text else None
            except Exception:
                pass
        return None
    except Exception:
        return None

# ---------- Result ----------

@dataclass
class ChannelResult:
    channel: str
    status: str  # SUCCESS / NO_VIDEO / NO_TRANSCRIPT / API_ERROR
    video_id: Optional[str] = None
    title: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[str] = None
    end_time: Optional[str] = None
    note: Optional[str] = None
    transcript_chars: int = 0
    debug_candidates: List[str] = None

def process_channel(name: str, channel_id: str, api_key: str) -> ChannelResult:
    now = datetime.now(timezone.utc)
    published_after = now - timedelta(hours=SEARCH_HOURS)

    try:
        items = youtube_search_latest(channel_id, published_after, api_key, max_results=10)
        if not items:
            return ChannelResult(channel=name, status="NO_VIDEO", note=f"최근 {SEARCH_HOURS}시간 검색 결과 없음", debug_candidates=[])

        video_ids = []
        for it in items:
            vid = it.get("id", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        details = youtube_videos_details(video_ids, api_key)

        # 후보 디버그 목록 만들기
        debug = []
        for vid in video_ids:
            d = details.get(vid)
            if not d:
                continue
            sn = d.get("snippet", {})
            title = sn.get("title", "")
            pub = sn.get("publishedAt")
            eff = effective_time_for_filter(d)
            lsd = d.get("liveStreamingDetails") or {}
            endt = lsd.get("actualEndTime")
            ongoing = is_live_ongoing(d)
            debug.append(f"{vid} | ongoing={ongoing} | publishedAt={pub} | actualEndTime={endt} | effective={eff} | {title[:60]}")

        # 선택 로직: 진행중 라이브 제외 + effective_time 기준 12시간
        chosen_id = None
        for vid in video_ids:
            d = details.get(vid)
            if not d:
                continue
            if is_live_ongoing(d):
                continue
            eff = effective_time_for_filter(d)
            if not eff:
                continue
            if (now - eff) > timedelta(hours=RECENT_HOURS):
                continue
            chosen_id = vid
            break

        if not chosen_id:
            return ChannelResult(
                channel=name,
                status="NO_VIDEO",
                note=f"최근 {RECENT_HOURS}시간 내 종료된 콘텐츠 없음(진행중 라이브 제외, 종료시간/게시시간 기준)",
                debug_candidates=debug
            )

        d = details[chosen_id]
        sn = d.get("snippet", {})
        lsd = d.get("liveStreamingDetails") or {}
        title = sn.get("title")
        pub = sn.get("publishedAt")
        endt = lsd.get("actualEndTime")
        url = f"https://www.youtube.com/watch?v={chosen_id}"

        text = fetch_transcript_text(chosen_id)
        if text:
            return ChannelResult(
                channel=name,
                status="SUCCESS",
                video_id=chosen_id,
                title=title,
                url=url,
                published_at=pub,
                end_time=endt,
                transcript_chars=len(text),
                note="자막 기반 텍스트 추출 성공",
                debug_candidates=debug
            )
        else:
            return ChannelResult(
                channel=name,
                status="NO_TRANSCRIPT",
                video_id=chosen_id,
                title=title,
                url=url,
                published_at=pub,
                end_time=endt,
                note="자막 없음/비활성화(Transcript 불가)",
                debug_candidates=debug
            )

    except Exception as e:
        return ChannelResult(channel=name, status="API_ERROR", note=str(e)[:300], debug_candidates=[])

def main():
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY가 설정되지 않았습니다(GitHub Secrets 확인).")

    run_id = os.getenv("GITHUB_RUN_ID", "LOCAL")
    run_num = os.getenv("GITHUB_RUN_NUMBER", "0")

    results: List[ChannelResult] = []
    for name, cid in CHANNELS.items():
        results.append(process_channel(name, cid, api_key))

    lines: List[str] = []
    lines.append("[미국 주식 시황 리포트 - 안정형]")
    lines.append(f"생성 시각: {kst_now_str()}")
    lines.append(f"Run: {run_num} (id={run_id})")
    lines.append(f"검색: 최근 {SEARCH_HOURS}시간 / 최종 선택: 최근 {RECENT_HOURS}시간")
    lines.append("필터 시간 기준: 종료된 라이브는 actualEndTime, 그 외는 publishedAt")
    lines.append("진행중 라이브는 제외")
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
            lines.append(f"  publishedAt: {r.published_at}")
        if r.end_time:
            lines.append(f"  actualEndTime: {r.end_time}")
        if r.status == "SUCCESS":
            lines.append(f"  텍스트화: 성공(문자수 {r.transcript_chars})")
        if r.note:
            lines.append(f"  비고: {r.note}")

        # NO_VIDEO/NO_TRANSCRIPT일 때 왜 그런지 후보 10개를 바로 보여줌
        if r.status in ("NO_VIDEO", "NO_TRANSCRIPT") and r.debug_candidates:
            lines.append("  후보(최신 10개) 디버그:")
            for s in r.debug_candidates[:10]:
                lines.append(f"   - {s}")

        lines.append("")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print("report.txt 생성 완료")

if __name__ == "__main__":
    main()
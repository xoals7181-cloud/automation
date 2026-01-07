import sys
import traceback
from datetime import datetime, timedelta
from typing import List, Dict

# ===============================
# 기본 설정
# ===============================

NOW = datetime.utcnow()
TIME_WINDOW_HOURS = 12

CHANNELS = {
    "Bloomberg": "https://www.youtube.com/@BloombergTV/videos",
    "Meet Kevin": "https://www.youtube.com/@MeetKevin/videos",
    "오선의 미국 증시": "https://www.youtube.com/@oseon/videos",
    "설명왕 테이버": "https://www.youtube.com/@taber/videos",
    "미국주식에 미치다 TV": "https://www.youtube.com/@usstocktv/videos",
    "뉴욕주민": "https://www.youtube.com/@nyresident/videos",
    "미주은": "https://www.youtube.com/@mijueun/videos",
    "투자왕 김단테": "https://www.youtube.com/@kimdante/videos",
}

# ===============================
# 유틸 함수
# ===============================

def log(msg: str):
    print(f"[{datetime.now()}] {msg}")

def within_time_window(published: datetime) -> bool:
    return NOW - published <= timedelta(hours=TIME_WINDOW_HOURS)

# ===============================
# 유튜브 영상 수집 (실제 다운로드 X, 구조 확인용)
# ===============================

def fetch_latest_video(channel_name: str, channel_url: str) -> Dict:
    """
    실제 다운로드 전 단계.
    지금은 '영상이 있는지 없는지'만 판단하는 구조.
    """
    try:
        # ⚠️ 실제 yt-dlp 연동은 다음 단계에서 붙임
        # 지금은 구조 안정화가 목적

        # 예시 로직 (가짜 데이터)
        # ----------------------------------
        # 여기는 나중에 yt-dlp --dump-json으로 교체됨
        # ----------------------------------

        simulated_has_video = True  # 테스트용

        if not simulated_has_video:
            return {
                "status": "NO_VIDEO",
                "reason": "최근 12시간 내 업로드 없음"
            }

        return {
            "status": "OK",
            "title": "미국 증시 급변동 분석",
            "published_at": NOW - timedelta(hours=3),
            "summary": "미국 증시는 CPI 발표 이후 기술주 중심으로 변동성이 확대됨."
        }

    except Exception as e:
        return {
            "status": "ERROR",
            "reason": str(e),
            "trace": traceback.format_exc()
        }

# ===============================
# 리포트 생성
# ===============================

def generate_report(results: Dict[str, Dict]) -> str:
    lines = []

    lines.append("[전날 미국 증시 요약]\n")
    lines.append("■ 핵심 이슈 TOP 5")
    lines.append("1. CPI 이후 기술주 변동성 확대")
    lines.append("2. 금리 인하 기대 vs 연준 경계")
    lines.append("3. AI 관련주 차별화")
    lines.append("4. 환율 변동성 확대")
    lines.append("5. 단기 조정 우려\n")

    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("[전문가 시각 요약]\n")

    for channel, result in results.items():
        if result["status"] == "OK":
            lines.append(f"- {channel}")
            lines.append(f"  · 해석 요지: {result['summary']}\n")

        elif result["status"] == "NO_VIDEO":
            lines.append(f"- {channel}")
            lines.append("  · 최근 12시간 내 영상 없음\n")

        elif result["status"] == "ERROR":
            lines.append(f"- {channel}")
            lines.append("  · 오류로 인한 자료 누락\n")

    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("[AI 메타 판단]")
    lines.append("- 현재 시장의 성격: 변동성 확대 국면")
    lines.append("-

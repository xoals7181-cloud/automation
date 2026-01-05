# 1. 필요한 도구 설치
!pip install openai youtube-transcript-api pytube whisper

# 2. API 키 입력
OPENAI_API_KEY = "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"
YOUTUBE_API_KEY = "AIzaSyCF0WV_SiKHqDdUuujUb1hhrHzwvRE3CgA"

# 3. 채널 목록
CHANNELS = {
    "설명왕_테이버": "UCkOonf-L0nS_Ea-v3-eG_OA",
    "소수몽키": "UC86H9Xp9uS7N-uYI0_j3S_Q",
    "오선": "UCRfB5KWhL2P_U-fI0m6A2Ag"
}

# 4. 오늘 영상 가져오기 → 자막 → 요약
print("오늘의 증시 요약 생성 중...")

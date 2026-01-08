from datetime import datetime, timedelta
import subprocess
import os

REPORT_FILE = "report.txt"

YOUTUBE_CHANNELS = {
    "Bloomberg": "https://www.youtube.com/@BloombergTV",
    "Meet Kevin": "https://www.youtube.com/@MeetKevin",
    "오선의 미국 증시": "https://www.youtube.com/@osunstock",
    "설명왕 테이버": "https://www.youtube.com/@taver",
    "뉴욕주민": "https://www.youtube.com/@nyresident"
}

def write_line(text=""):
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(text + "\n")

def header():
    write_line(f"[전날 미국 증시 요약]")
    write_line(f"생성 시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    write_line("=" * 50)
    write_line()

def check_recent_video(channel_name, channel_url):
    try:
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--print",
            "%(upload_date)s|%(title)s",
            channel_url
        ]
        result = subprocess.check_output(cmd, text=True)
        lines = result.strip().split("\n")

        now = datetime.utcnow()
        for line in lines:
            date_str, title = line.split("|", 1)
            upload_time = datetime.strptime(date_str, "%Y%m%d")
            if now - upload_time <= timedelta(hours=12):
                return f"✔ 최근 영상 있음: {title}"

        return "영상 없음 (최근 12시간)"

    except Exception as e:
        return "오류로 인한 누락"

def main():
    if os.path.exists(REPORT_FILE):
        os.remove(REPORT_FILE)

    header()

    write_line("■ 유튜브 채널 체크")
    for name, url in YOUTUBE_CHANNELS.items():
        status = check_recent_video(name, url)
        write_line(f"- {name}: {status}")

    write_line()
    write_line("━━━━━━━━━━━━━━━━━━")
    write_line("[기타 사항]")
    write_line("자동 생성 리포트입니다.")

if __name__ == "__main__":
    main()
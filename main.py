from datetime import datetime

def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    lines = []
    lines.append("[전날 미국 증시 요약]")
    lines.append("")
    lines.append("■ 생성 시각")
    lines.append(f"- {now}")
    lines.append("")
    lines.append("■ 핵심 이슈 TOP 5")
    for i in range(1, 6):
        lines.append(f"{i}. 데이터 수집 예정")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("[기타 사항]")
    lines.append("- 현재는 자동화 구조 검증 단계입니다.")

    with open("report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("report.txt 생성 완료")

if __name__ == "__main__":
    main()

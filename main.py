from datetime import datetime

def generate_report():
    today = datetime.now().strftime("%Y-%m-%d")

    lines = []
    lines.append(f"미국 증시 리포트 ({today})")
    lines.append("-" * 30)
    lines.append("1. 시장 요약")
    lines.append("- 다우, 나스닥, S&P500 혼조세")
    lines.append("")
    lines.append("2. 주요 이슈")
    lines.append("- 금리 동결 기대감 지속")
    lines.append("- AI 관련 종목 변동성 확대")
    lines.append("")
    lines.append("3. 투자 심리")
    lines.append("- 단기 경계, 중장기 관망")

    report = "\n".join(lines)

    with open("report.txt", "w", encoding="utf-8") as f:
        f.write(report)

    print("리포트 생성 완료")

if __name__ == "__main__":
    generate_report()

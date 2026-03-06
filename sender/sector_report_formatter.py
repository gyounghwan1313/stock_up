"""섹터 트렌드 분석 리포트 Slack 메시지 포매터."""

from datetime import datetime

from engine.sector_trend import SectorTrendReport


def format_sector_trend_report(report: SectorTrendReport) -> str:
    today = report.generated_at.strftime("%Y-%m-%d")
    lines = [
        f":bar_chart: *섹터 트렌드 분석 리포트* ({report.analysis_period_days}일간 | {today})",
        "━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # 상승 전망 섹터
    if report.trending_categories:
        lines.append("*:fire: 상승 전망 섹터*")
        for i, cat in enumerate(report.trending_categories, 1):
            arrow = "↗ 상승세" if cat.trend_direction > 0.05 else (
                "↘ 하락세" if cat.trend_direction < -0.05 else "→ 유지"
            )
            lines.append(
                f"{i}. *{cat.category}* — 감성 {cat.avg_sentiment:+.2f} "
                f"({cat.news_count}건) {arrow}"
            )
            for title, score in cat.top_headlines[:2]:
                # 제목이 너무 길면 자르기
                display_title = title[:60] + "..." if len(title) > 60 else title
                lines.append(f"   • \"{display_title}\" ({score:+.1f})")
            lines.append("")
    else:
        lines.append("_상승 전망 카테고리 없음_")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")

    # 저평가 종목 발굴
    if report.sector_candidates:
        lines.append("*:gem: 저평가 종목 발굴*")
        lines.append("")
        for sector_name, stocks in report.sector_candidates.items():
            if not stocks:
                continue
            lines.append(f"_{sector_name} 섹터:_")
            for s in stocks:
                lines.append(f"• *{s.symbol}* ({s.name}) — ${s.price:.2f}")
                details = []
                if s.per is not None:
                    details.append(f"PER {s.per:.1f}")
                if s.pbr is not None:
                    details.append(f"PBR {s.pbr:.1f}")
                if s.rsi is not None:
                    details.append(f"RSI {s.rsi:.1f}")
                if s.bollinger_position != "중간":
                    details.append(s.bollinger_position)
                if details:
                    lines.append(f"  {' | '.join(details)}")
            lines.append("")
    else:
        lines.append("_조건을 충족하는 저평가 종목 없음_")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")

    # AI 분석 요약
    lines.append("*:robot_face: AI 분석 요약*")
    lines.append(report.ai_narrative)

    return "\n".join(lines)

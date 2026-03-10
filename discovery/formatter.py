def format_discovery_message(discovered: list) -> str:
    if not discovered:
        return ""
    lines = [":mag: *발굴 종목*"]
    for s in discovered[:10]:
        lines.append(f"  • *{s.symbol}* ({s.name}) — {s.discovery_reason}")
    return "\n".join(lines)

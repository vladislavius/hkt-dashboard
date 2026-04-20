"""Общие форматтеры для вывода."""
from core.mappings import COUNTRY_FLAGS


def fmt_top10(countries):
    """Форматирует топ-10 стран по числу рейсов для Telegram-сообщения."""
    lines = []
    sorted_c = sorted(countries.items(), key=lambda x: x[1]["flights"], reverse=True)[:10]
    for i, (c, v) in enumerate(sorted_c, 1):
        flag = COUNTRY_FLAGS.get(v.get("country", c), "")
        lines.append(f"{i}. {flag} {c}: {v['flights']} рейсов (~{v['pax']:,} пасс.)")
    return "\n".join(lines) if lines else "Нет данных"

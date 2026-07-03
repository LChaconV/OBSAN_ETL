"""
core/format_utils.py — Formateo compartido de valores numéricos para la UI.
"""


def format_cop(value, decimals: int = 1) -> str:
    """
    Formatea un monto en pesos colombianos corrientes (COP)

    """
    value = float(value)
    sign  = "-" if value < 0 else ""
    value = abs(value)

    if value >= 1_000_000_000_000:
        return f"{sign}${value / 1_000_000_000_000:,.{decimals}f} billones COP"
    if value >= 1_000_000_000:
        return f"{sign}${value / 1_000_000_000:,.{decimals}f} mil millones COP"
    if value >= 1_000_000:
        return f"{sign}${value / 1_000_000:,.{decimals}f} millones COP"
    return f"{sign}${value:,.0f} COP"

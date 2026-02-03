"""
Utilitários para formatação de números no padrão brasileiro (BRL).

Padrão BRL:
- Separador de milhares: ponto (.)
- Separador decimal: vírgula (,)
- Exemplos: 1.234,56 | 12.345.678,90
"""

from typing import Optional


def format_number_br(
    value: float,
    decimals: int = 0,
    unit: str = "",
    prefix: str = ""
) -> str:
    """
    Formata número no padrão brasileiro.

    Args:
        value: Valor numérico
        decimals: Número de casas decimais (padrão: 0)
        unit: Unidade a adicionar após o valor (ex: " MW", " kW")
        prefix: Prefixo antes do valor (ex: "R$ ")

    Returns:
        String formatada no padrão BRL

    Exemplos:
        >>> format_number_br(1234.56)
        '1.235'
        >>> format_number_br(1234.56, decimals=2)
        '1.234,56'
        >>> format_number_br(1234.56, decimals=1, unit=" MW")
        '1.234,6 MW'
        >>> format_number_br(1234.56, decimals=2, prefix="R$ ")
        'R$ 1.234,56'
    """
    if value is None:
        return "N/A"

    # Formatar com casas decimais
    formatted = f"{value:,.{decimals}f}"

    # Substituir separadores: , → TEMP, . → ,, TEMP → .
    formatted = formatted.replace(",", "TEMP")
    formatted = formatted.replace(".", ",")
    formatted = formatted.replace("TEMP", ".")

    # Adicionar prefixo e unidade
    return f"{prefix}{formatted}{unit}"


def format_mw(value: float, decimals: int = 1) -> str:
    """
    Formata valor em MW (megawatts).

    Args:
        value: Valor em MW
        decimals: Casas decimais (padrão: 1)

    Returns:
        String formatada (ex: "1.234,5 MW")
    """
    return format_number_br(value, decimals=decimals, unit=" MW")


def format_kw(value: float, decimals: int = 1) -> str:
    """
    Formata valor em kW (kilowatts).

    Args:
        value: Valor em kW
        decimals: Casas decimais (padrão: 1)

    Returns:
        String formatada (ex: "123,4 kW")
    """
    return format_number_br(value, decimals=decimals, unit=" kW")


def format_wm2(value: float, decimals: int = 0) -> str:
    """
    Formata irradiância em W/m².

    Args:
        value: Valor em W/m²
        decimals: Casas decimais (padrão: 0)

    Returns:
        String formatada (ex: "850 W/m²")
    """
    return format_number_br(value, decimals=decimals, unit=" W/m²")


def format_temperature(value: float, decimals: int = 1) -> str:
    """
    Formata temperatura em °C.

    Args:
        value: Valor em °C
        decimals: Casas decimais (padrão: 1)

    Returns:
        String formatada (ex: "28,5°C")
    """
    return format_number_br(value, decimals=decimals, unit="°C")


def format_percentage(value: float, decimals: int = 1) -> str:
    """
    Formata percentual.

    Args:
        value: Valor em percentual (0-100)
        decimals: Casas decimais (padrão: 1)

    Returns:
        String formatada (ex: "85,3%")
    """
    return format_number_br(value, decimals=decimals, unit="%")


def format_factor(value: float, decimals: int = 2) -> str:
    """
    Formata fator de carga (p.u.).

    Args:
        value: Valor em p.u. (0.0-2.0)
        decimals: Casas decimais (padrão: 2)

    Returns:
        String formatada (ex: "1,45 p.u.")
    """
    return format_number_br(value, decimals=decimals, unit=" p.u.")


def format_integer(value: int) -> str:
    """
    Formata número inteiro (quantidade, contagem).

    Args:
        value: Valor inteiro

    Returns:
        String formatada (ex: "1.234")
    """
    return format_number_br(value, decimals=0)


def format_currency(value: float, decimals: int = 2) -> str:
    """
    Formata valor monetário em reais.

    Args:
        value: Valor em R$
        decimals: Casas decimais (padrão: 2)

    Returns:
        String formatada (ex: "R$ 1.234,56")
    """
    return format_number_br(value, decimals=decimals, prefix="R$ ")


def format_distance(value: float, decimals: int = 1) -> str:
    """
    Formata distância em km.

    Args:
        value: Valor em km
        decimals: Casas decimais (padrão: 1)

    Returns:
        String formatada (ex: "10,5 km")
    """
    return format_number_br(value, decimals=decimals, unit=" km")


def format_voltage(value: float, decimals: int = 0) -> str:
    """
    Formata tensão em kV.

    Args:
        value: Valor em kV
        decimals: Casas decimais (padrão: 0)

    Returns:
        String formatada (ex: "138 kV")
    """
    return format_number_br(value, decimals=decimals, unit=" kV")


def format_compact(value: float, decimals: int = 1) -> str:
    """
    Formata número de forma compacta (K, M, G).

    Args:
        value: Valor numérico
        decimals: Casas decimais (padrão: 1)

    Returns:
        String formatada compacta

    Exemplos:
        >>> format_compact(1234)
        '1,2 K'
        >>> format_compact(1234567)
        '1,2 M'
        >>> format_compact(1234567890)
        '1,2 G'
    """
    if abs(value) >= 1_000_000_000:
        return format_number_br(value / 1_000_000_000, decimals=decimals, unit=" G")
    elif abs(value) >= 1_000_000:
        return format_number_br(value / 1_000_000, decimals=decimals, unit=" M")
    elif abs(value) >= 1_000:
        return format_number_br(value / 1_000, decimals=decimals, unit=" K")
    else:
        return format_number_br(value, decimals=decimals)


# Configuração de locale para Plotly
def get_plotly_locale_config() -> dict:
    """
    Retorna configuração de locale brasileiro para Plotly.

    Returns:
        Dict com configuração de separadores e formato de datas
    """
    return {
        "separators": ",.",  # Decimal (vírgula), Milhares (ponto)
        "decimal": ",",
        "thousands": ".",
        "grouping": [3],
        "currency": ["R$", ""],
    }


def apply_plotly_locale(fig):
    """
    Aplica formatação brasileira aos números de um gráfico Plotly.

    Args:
        fig: Figura Plotly

    Returns:
        Figura com formatação brasileira aplicada

    Exemplo:
        >>> fig = go.Figure(...)
        >>> fig = apply_plotly_locale(fig)
        >>> st.plotly_chart(fig)
    """
    fig.update_layout(
        separators=",.",  # Primeiro é decimal, segundo é milhares
    )
    return fig


# Formatação para DataFrames do Pandas/Streamlit
def format_dataframe_numbers(df, columns_config: Optional[dict] = None):
    """
    Formata números de um DataFrame no padrão brasileiro.

    Args:
        df: DataFrame pandas
        columns_config: Dict com configuração de formatação por coluna
                       Ex: {"potencia_mw": {"decimals": 2, "unit": " MW"}}

    Returns:
        DataFrame com colunas formatadas

    Exemplo:
        >>> df = pd.DataFrame({"valor_mw": [1234.56, 789.01]})
        >>> df_formatted = format_dataframe_numbers(df, {
        ...     "valor_mw": {"decimals": 2, "unit": " MW"}
        ... })
        >>> st.dataframe(df_formatted)
    """
    df_copy = df.copy()

    if columns_config:
        for col, config in columns_config.items():
            if col in df_copy.columns:
                decimals = config.get("decimals", 2)
                unit = config.get("unit", "")
                df_copy[col] = df_copy[col].apply(
                    lambda x: format_number_br(x, decimals=decimals, unit=unit)
                    if pd.notna(x) else "N/A"
                )

    return df_copy


if __name__ == "__main__":
    # Testes
    print("=== Testes de Formatação Brasileira ===\n")

    print("Números gerais:")
    print(f"  1234.56 → {format_number_br(1234.56, decimals=2)}")
    print(f"  1234567.89 → {format_number_br(1234567.89, decimals=2)}")
    print(f"  0.123 → {format_number_br(0.123, decimals=3)}")
    print()

    print("Potência:")
    print(f"  150.5 MW → {format_mw(150.5)}")
    print(f"  1234.56 kW → {format_kw(1234.56)}")
    print()

    print("Irradiância:")
    print(f"  850 W/m² → {format_wm2(850)}")
    print(f"  1234.56 W/m² → {format_wm2(1234.56, decimals=1)}")
    print()

    print("Temperatura:")
    print(f"  28.5°C → {format_temperature(28.5)}")
    print()

    print("Percentual:")
    print(f"  85.3% → {format_percentage(85.3)}")
    print()

    print("Fator:")
    print(f"  1.45 p.u. → {format_factor(1.45)}")
    print()

    print("Inteiros:")
    print(f"  1234 → {format_integer(1234)}")
    print(f"  1234567 → {format_integer(1234567)}")
    print()

    print("Moeda:")
    print(f"  R$ 1234.56 → {format_currency(1234.56)}")
    print()

    print("Compacto:")
    print(f"  1234 → {format_compact(1234)}")
    print(f"  1234567 → {format_compact(1234567)}")
    print(f"  1234567890 → {format_compact(1234567890)}")

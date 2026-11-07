"""Números: leitura sem perdas e sem adivinhar.

* `JsonFloat`: quando um número do JSON tem mais de 15 algarismos significativos (e o double pode ter perdido
  algum), o leitor guarda também o texto original (`0.12345678901234567890123`). Comporta-se como um float em
  tudo; o cast para decimal/bigint usa o texto e fica exacto. Com até 15 algarismos o double preserva o valor
  (garantia DBL_DIG), por isso fica um float normal — sem custo de memória.
* `parse_number`: a única função que converte texto em número. Aceita só a gramática dos números JSON
  (`123`, `-1.5`, `2e3`), com dígitos ASCII, e recusa o que é ambíguo ou não é um número:

      "1.234", "12.345"     ambíguo (pode ser separador de milhares)   -> erro, a não ser com locale declarado
      "1,5", "1,234"        vírgula                                    -> erro, a não ser com locale declarado
      "1_000", "+5", "007", "0x10", "١٢٣", "１２３", "NaN", "inf"      -> erro
      " 1.5 "               espaços ASCII à volta                       -> 1.5

  Com `decimal`/`thousands` declarados (ex.: to_number: {decimal: ",", thousands: "."}), os separadores de
  milhares têm de estar em grupos de 3 dígitos.
"""
from __future__ import annotations

import math
import re
from decimal import (ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP,
                     Decimal, InvalidOperation)

from .notes import note

JSON_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", re.ASCII)
# "1.234", "-12.345", "1,234", "1.234.567": podem ser milhares (pt/de: 1.234 = 1234) ou decimais (en: 1.234)
AMBIGUOUS = re.compile(r"-?[1-9][0-9]{0,2}(?:([.,])[0-9]{3})(?:\1[0-9]{3})*", re.ASCII)
ASCII_SPACE = " \t\r\n"
DECIMAL_SEPARATORS = {".", ","}
THOUSANDS_SEPARATORS = {".", ",", " ", "'", "\u00a0", "\u202f"}
FORMAT_HINT = "declare o formato com to_number: {decimal: ',', thousands: '.'} (ou {decimal: '.', thousands: ','})"


class NumberError(ValueError):
    """Texto que não é um número, ou que não pode ser lido sem adivinhar."""


def short(value) -> str:
    text = value.text if isinstance(value, TextFloat) else repr(value)   # número do documento: como foi escrito
    return text if len(text) <= 80 else text[:80] + "…"


class TextFloat(float):
    """float que guarda também o valor decimal exacto em `.text`."""

    __slots__ = ("text",)

    def __new__(cls, text):
        obj = super().__new__(cls, text)
        obj.text = text if isinstance(text, str) else repr(float(text))
        return obj

    def __getnewargs__(self):  # pickle/deepcopy mantêm o texto
        return (self.text,)


class JsonFloat(TextFloat):
    """Número tal como escrito no documento (mais de 15 algarismos): está sujeito à regra "o double tem de o
    reproduzir" (ver to_float)."""


class ExactFloat(TextFloat):
    """Resultado exacto de um cálculo (sum, multiply, unit_convert...) com mais de 15 algarismos: um decimal
    recebe-o sem perdas; num double fica o mais próximo (o tipo double foi declarado para um valor calculado)."""


MODES = {"half_even": ROUND_HALF_EVEN, "half_up": ROUND_HALF_UP, "half_down": ROUND_HALF_DOWN, "up": ROUND_UP,
         "down": ROUND_DOWN, "ceiling": ROUND_CEILING, "floor": ROUND_FLOOR}


def computed(d: Decimal) -> float:
    """Resultado de um cálculo em Decimal -> float (ExactFloat se tiver mais de 15 algarismos). -0 fica 0."""
    if not d.is_finite():
        raise NumberError(f"resultado não finito ({d})")
    if d == 0:
        return 0.0
    f = float(d)
    if math.isinf(f):
        raise NumberError(f"o resultado {d} está fora do alcance de um double")
    if f == 0:
        raise NumberError(f"o resultado {d} é demasiado pequeno para um double (ficaria 0)")
    return f if len(d.normalize().as_tuple().digits) <= DBL_DIG else ExactFloat(str(d))


DBL_DIG = 15   # algarismos significativos que um double preserva sempre


def json_float(text: str) -> float:
    """parse_float do leitor de JSON: recusa números que o double não representa (1e400, 1e-400) e guarda o
    texto (JsonFloat) só quando há mais de 15 algarismos significativos."""
    value = float(text)
    if math.isinf(value):
        raise NumberError(f"o número {text} está fora do alcance de um double")
    if value == 0 and _significant_digits(text):
        raise NumberError(f"o número {text} é demasiado pequeno para um double (ficaria 0)")
    if len(text) <= DBL_DIG or _significant_digits(text) <= DBL_DIG:   # len: atalho (sinal/ponto só reduzem)
        return value
    return JsonFloat(text)


def _significant_digits(text: str) -> int:
    mantissa = re.split(r"[eE]", text, maxsplit=1)[0]
    return len(mantissa.replace("-", "").replace(".", "").strip("0"))


def check_format(decimal: str | None = None, thousands: str | None = None) -> None:
    """Valida os argumentos de formato (usado ao carregar a configuração)."""
    if decimal is not None and decimal not in DECIMAL_SEPARATORS:
        raise NumberError(f"decimal deve ser um de {sorted(DECIMAL_SEPARATORS)}, não {decimal!r}")
    if thousands is not None and thousands not in THOUSANDS_SEPARATORS:
        raise NumberError(f"thousands deve ser um de {sorted(THOUSANDS_SEPARATORS)}, não {thousands!r}")
    if thousands is not None and thousands == (decimal or "."):
        raise NumberError("decimal e thousands não podem ser o mesmo separador")


def parse_number(text: str, decimal: str | None = None, thousands: str | None = None) -> Decimal:
    """Texto -> Decimal exacto, ou NumberError com a razão. Sem locale declarado aplica a gramática JSON."""
    if not isinstance(text, str):
        raise NumberError(f"{short(text)} não é texto")
    s = text.strip(ASCII_SPACE)
    if not s:
        raise NumberError("texto vazio não é um número")
    if decimal is None and thousands is None:
        if AMBIGUOUS.fullmatch(s):
            raise NumberError(f"{text!r} é ambíguo: o separador pode ser de milhares ou decimal; {FORMAT_HINT}")
        if "," in s:
            raise NumberError(f"{text!r} usa vírgula; {FORMAT_HINT}")
        normal = s
    else:
        check_format(decimal, thousands)
        normal = _apply_format(text, s, decimal or ".", thousands)
    if not JSON_NUMBER.fullmatch(normal):
        low = normal.lower().lstrip("+-")
        if low in ("nan", "inf", "infinity"):
            raise NumberError(f"{text!r} não é um número finito")
        raise NumberError(f"{text!r} não é um número (formato aceite: 123, -1.5, 2e3; só dígitos 0-9)")
    try:
        return Decimal(normal)
    except InvalidOperation as e:  # pragma: no cover - a gramática já o garante
        raise NumberError(f"{text!r} não é um número") from e


def _apply_format(text: str, s: str, decimal: str, thousands: str | None) -> str:
    sign = ""
    if s[:1] == "-":
        sign, s = "-", s[1:]
    if decimal in s:
        whole, _, frac = s.partition(decimal)
        if decimal in frac:
            raise NumberError(f"{text!r} tem mais de um separador decimal {decimal!r}")
        frac = "." + frac
    else:
        whole, frac = s, ""
    if thousands and thousands in whole:
        groups = whole.split(thousands)
        if not (1 <= len(groups[0]) <= 3 and all(len(g) == 3 for g in groups[1:])):
            raise NumberError(f"{text!r}: separador de milhares {thousands!r} fora de grupos de 3 dígitos")
        whole = "".join(groups)
    other = ({".", ","} - {decimal, thousands})
    if any(o in whole + frac[1:] for o in other):
        raise NumberError(f"{text!r} tem um separador que não foi declarado")
    return sign + whole + frac


def to_float(value, rounding: str | None = None, single: bool = False) -> float:
    """Número ou texto -> float finito (double; `single`: float de 32 bits).

    Um número escrito (texto, número do documento, Decimal do to_number, inteiro) tem de sair do double tal como
    foi escrito: 2^53+1 ou 0.12345678901234567890 dão erro, a não ser com `rounding: nearest` na coluna (perda
    declarada e contada). Floats já calculados (ExactFloat, resultados de transformações) não são verificados.
    Booleanos não são números.
    """
    if isinstance(value, bool):
        raise NumberError(f"o booleano {str(value).lower()} não é um número")
    if type(value) is int and -(2 ** 53) <= value <= 2 ** 53 and not single:
        return float(value)                   # atalho: inteiros até 2^53 são sempre exactos num double
    written: Decimal | None
    if isinstance(value, str):
        written = parse_number(value)
    elif isinstance(value, JsonFloat):
        written = Decimal(value.text)
    elif isinstance(value, Decimal):
        written = value
    elif isinstance(value, int):
        written = Decimal(value)
    elif isinstance(value, float):
        written = None
    else:
        raise NumberError(f"{short(value)} não é um número")
    if written is not None and not written.is_finite():
        raise NumberError(f"{short(value)} não é um número finito")
    try:
        f = float(written) if written is not None else float(value)
    except OverflowError:
        f = math.inf
    if not math.isfinite(f):
        raise NumberError(f"{short(value)} está fora do alcance de um double" if written is not None
                          else f"{short(value)} não é um número finito")
    if f == 0 and written is not None and written != 0:
        raise NumberError(f"{short(value)} é demasiado pequeno para um double (ficaria 0)")
    exact = written if written is not None else Decimal(repr(f))
    if written is not None and Decimal(repr(f)) != written:
        if rounding is None:
            raise NumberError(f"{short(value)} não cabe exactamente num double (ficaria {f!r}); "
                              f"declare rounding: nearest na coluna para aceitar o mais próximo")
        note("declared", "rounded", "rounded", f"{short(value)} guardado como {f!r} (double mais próximo)")
    if single:
        import numpy as np  # noqa: PLC0415 - só para float de 32 bits

        with np.errstate(over="ignore"):          # o overflow é tratado já a seguir (erro em português)
            f32 = np.float32(f)
        if not math.isfinite(float(f32)):
            raise NumberError(f"{short(value)} está fora do alcance de um float (32 bits)")
        if Decimal(str(f32)) != exact:
            if rounding is None:
                raise NumberError(f"{short(value)} não cabe num float de 32 bits (ficaria {str(f32)}); use type: "
                                  f"double, ou declare rounding: nearest")
            note("declared", "rounded", "rounded", f"{short(value)} guardado como {str(f32)} (float de 32 bits)")
        return float(f32)
    return f


def to_decimal(value) -> Decimal:
    """Número ou texto -> Decimal exacto (números com texto guardado usam-no; outros floats, o seu repr)."""
    if isinstance(value, bool):
        raise NumberError(f"o booleano {str(value).lower()} não é um número")
    if isinstance(value, TextFloat):
        return Decimal(value.text)
    if isinstance(value, str):
        return parse_number(value)
    if isinstance(value, (int, Decimal)):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(repr(value))
    raise NumberError(f"{short(value)} não é um número")

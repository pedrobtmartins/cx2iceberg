"""Numbers: read without losses and without guessing.

* `JsonFloat`: when a JSON number has more than 15 significant digits (and the double may have lost
  some), the reader also keeps the original text (`0.12345678901234567890123`). It behaves like a float in
  everything; the cast to decimal/bigint uses the text and stays exact. With up to 15 digits the double keeps the
  value (the DBL_DIG guarantee), so it stays a plain float — no memory cost.
* `parse_number`: the only function that turns text into a number. It accepts only the grammar of JSON numbers
  (`123`, `-1.5`, `2e3`), with ASCII digits, and refuses what is ambiguous or not a number:

      "1.234", "12.345"     ambiguous (may be a thousands separator)   -> error, unless a locale is declared
      "1,5", "1,234"        comma                                      -> error, unless a locale is declared
      "1_000", "+5", "007", "0x10", "١٢٣", "１２３", "NaN", "inf"      -> error
      " 1.5 "               ASCII spaces around it                      -> 1.5

  With `decimal`/`thousands` declared (e.g. to_number: {decimal: ",", thousands: "."}), the thousands
  separators must be in groups of 3 digits.
"""
from __future__ import annotations

import math
import re
from decimal import (ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP,
                     Decimal, InvalidOperation)

from .notes import note

JSON_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", re.ASCII)
# "1.234", "-12.345", "1,234", "1.234.567": may be thousands (pt/de: 1.234 = 1234) or decimals (en: 1.234)
AMBIGUOUS = re.compile(r"-?[1-9][0-9]{0,2}(?:([.,])[0-9]{3})(?:\1[0-9]{3})*", re.ASCII)
ASCII_SPACE = " \t\r\n"
DECIMAL_SEPARATORS = {".", ","}
THOUSANDS_SEPARATORS = {".", ",", " ", "'", "\u00a0", "\u202f"}
FORMAT_HINT = "declare the format with to_number: {decimal: ',', thousands: '.'} (or {decimal: '.', thousands: ','})"


class NumberError(ValueError):
    """Text that is not a number, or that cannot be read without guessing."""


def short(value) -> str:
    text = value.text if isinstance(value, TextFloat) else repr(value)   # a document number: as it was written
    return text if len(text) <= 80 else text[:80] + "…"


class TextFloat(float):
    """float that also keeps the exact decimal value in `.text`."""

    __slots__ = ("text",)

    def __new__(cls, text):
        obj = super().__new__(cls, text)
        obj.text = text if isinstance(text, str) else repr(float(text))
        return obj

    def __getnewargs__(self):  # pickle/deepcopy keep the text
        return (self.text,)


class JsonFloat(TextFloat):
    """Number as written in the document (more than 15 digits): it is subject to the rule "the double must
    reproduce it" (see to_float)."""


class ExactFloat(TextFloat):
    """Exact result of a computation (sum, multiply, unit_convert...) with more than 15 digits: a decimal
    takes it without losses; in a double the nearest value is kept (the double type was declared for a computed value)."""


MODES = {"half_even": ROUND_HALF_EVEN, "half_up": ROUND_HALF_UP, "half_down": ROUND_HALF_DOWN, "up": ROUND_UP,
         "down": ROUND_DOWN, "ceiling": ROUND_CEILING, "floor": ROUND_FLOOR}


def computed(d: Decimal) -> float:
    """Result of a Decimal computation -> float (ExactFloat if it has more than 15 digits). -0 becomes 0."""
    if not d.is_finite():
        raise NumberError(f"non-finite result ({d})")
    if d == 0:
        return 0.0
    f = float(d)
    if math.isinf(f):
        raise NumberError(f"the result {d} is out of range for a double")
    if f == 0:
        raise NumberError(f"the result {d} is too small for a double (it would become 0)")
    return f if len(d.normalize().as_tuple().digits) <= DBL_DIG else ExactFloat(str(d))


DBL_DIG = 15   # significant digits a double always preserves


def json_float(text: str) -> float:
    """The JSON reader's parse_float: refuses numbers the double cannot represent (1e400, 1e-400) and keeps the
    text (JsonFloat) only when there are more than 15 significant digits."""
    value = float(text)
    if math.isinf(value):
        raise NumberError(f"the number {text} is out of range for a double")
    if value == 0 and _significant_digits(text):
        raise NumberError(f"the number {text} is too small for a double (it would become 0)")
    if len(text) <= DBL_DIG or _significant_digits(text) <= DBL_DIG:   # len: shortcut (sign/point only reduce it)
        return value
    return JsonFloat(text)


def _significant_digits(text: str) -> int:
    mantissa = re.split(r"[eE]", text, maxsplit=1)[0]
    return len(mantissa.replace("-", "").replace(".", "").strip("0"))


def check_format(decimal: str | None = None, thousands: str | None = None) -> None:
    """Validates the format arguments (used when the configuration is loaded)."""
    if decimal is not None and decimal not in DECIMAL_SEPARATORS:
        raise NumberError(f"decimal must be one of {sorted(DECIMAL_SEPARATORS)}, not {decimal!r}")
    if thousands is not None and thousands not in THOUSANDS_SEPARATORS:
        raise NumberError(f"thousands must be one of {sorted(THOUSANDS_SEPARATORS)}, not {thousands!r}")
    if thousands is not None and thousands == (decimal or "."):
        raise NumberError("decimal and thousands cannot be the same separator")


def parse_number(text: str, decimal: str | None = None, thousands: str | None = None) -> Decimal:
    """Text -> exact Decimal, or NumberError with the reason. Without a declared locale the JSON grammar applies."""
    if not isinstance(text, str):
        raise NumberError(f"{short(text)} is not text")
    s = text.strip(ASCII_SPACE)
    if not s:
        raise NumberError("empty text is not a number")
    if decimal is None and thousands is None:
        if AMBIGUOUS.fullmatch(s):
            raise NumberError(f"{text!r} is ambiguous: the separator may be for thousands or decimals; {FORMAT_HINT}")
        if "," in s:
            raise NumberError(f"{text!r} uses a comma; {FORMAT_HINT}")
        normal = s
    else:
        check_format(decimal, thousands)
        normal = _apply_format(text, s, decimal or ".", thousands)
    if not JSON_NUMBER.fullmatch(normal):
        low = normal.lower().lstrip("+-")
        if low in ("nan", "inf", "infinity"):
            raise NumberError(f"{text!r} is not a finite number")
        raise NumberError(f"{text!r} is not a number (accepted format: 123, -1.5, 2e3; digits 0-9 only)")
    try:
        return Decimal(normal)
    except InvalidOperation as e:  # pragma: no cover - the grammar already ensures it
        raise NumberError(f"{text!r} is not a number") from e


def _apply_format(text: str, s: str, decimal: str, thousands: str | None) -> str:
    sign = ""
    if s[:1] == "-":
        sign, s = "-", s[1:]
    if decimal in s:
        whole, _, frac = s.partition(decimal)
        if decimal in frac:
            raise NumberError(f"{text!r} has more than one decimal separator {decimal!r}")
        frac = "." + frac
    else:
        whole, frac = s, ""
    if thousands and thousands in whole:
        groups = whole.split(thousands)
        if not (1 <= len(groups[0]) <= 3 and all(len(g) == 3 for g in groups[1:])):
            raise NumberError(f"{text!r}: thousands separator {thousands!r} outside groups of 3 digits")
        whole = "".join(groups)
    other = ({".", ","} - {decimal, thousands})
    if any(o in whole + frac[1:] for o in other):
        raise NumberError(f"{text!r} has a separator that was not declared")
    return sign + whole + frac


def to_float(value, rounding: str | None = None, single: bool = False) -> float:
    """Number or text -> finite float (double; `single`: 32-bit float).

    A written number (text, a document number, a Decimal from to_number, an integer) must come out of the double as
    it was written: 2^53+1 or 0.12345678901234567890 are errors, unless the column has `rounding: nearest` (a loss
    declared and counted). Already computed floats (ExactFloat, results of transformations) are not checked.
    Booleans are not numbers.
    """
    if isinstance(value, bool):
        raise NumberError(f"the boolean {str(value).lower()} is not a number")
    if type(value) is int and -(2 ** 53) <= value <= 2 ** 53 and not single:
        return float(value)                   # shortcut: integers up to 2^53 are always exact in a double
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
        raise NumberError(f"{short(value)} is not a number")
    if written is not None and not written.is_finite():
        raise NumberError(f"{short(value)} is not a finite number")
    try:
        f = float(written) if written is not None else float(value)
    except OverflowError:
        f = math.inf
    if not math.isfinite(f):
        raise NumberError(f"{short(value)} is out of range for a double" if written is not None
                          else f"{short(value)} is not a finite number")
    if f == 0 and written is not None and written != 0:
        raise NumberError(f"{short(value)} is too small for a double (it would become 0)")
    exact = written if written is not None else Decimal(repr(f))
    if written is not None and Decimal(repr(f)) != written:
        if rounding is None:
            raise NumberError(f"{short(value)} does not fit exactly in a double (it would become {f!r}); "
                              f"declare rounding: nearest on the column to accept the nearest value")
        note("declared", "rounded", "rounded", f"{short(value)} stored as {f!r} (nearest double)")
    if single:
        import numpy as np  # noqa: PLC0415 - only for 32-bit floats

        with np.errstate(over="ignore"):          # the overflow is handled right after (with a readable error)
            f32 = np.float32(f)
        if not math.isfinite(float(f32)):
            raise NumberError(f"{short(value)} is out of range for a float (32 bits)")
        if Decimal(str(f32)) != exact:
            if rounding is None:
                raise NumberError(f"{short(value)} does not fit in a 32-bit float (it would become {str(f32)}); use type: "
                                  f"double, or declare rounding: nearest")
            note("declared", "rounded", "rounded", f"{short(value)} stored as {str(f32)} (32-bit float)")
        return float(f32)
    return f


def to_decimal(value) -> Decimal:
    """Number or text -> exact Decimal (numbers with stored text use it; other floats, their repr)."""
    if isinstance(value, bool):
        raise NumberError(f"the boolean {str(value).lower()} is not a number")
    if isinstance(value, TextFloat):
        return Decimal(value.text)
    if isinstance(value, str):
        return parse_number(value)
    if isinstance(value, (int, Decimal)):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(repr(value))
    raise NumberError(f"{short(value)} is not a number")

"""Shared alert rules for tests and Spark streaming policy evaluation.
These are synthetic policy examples, NOT Sunset World approved forbidden phrases.
"""
from __future__ import annotations
import re
from dataclasses import dataclass

RULES={
 'guaranteed_return': r'\b(garantiz(?:o|amos)\s+(?:una\s+)?rentabilidad|rendimiento\s+garantizado)\b',
 'zero_risk': r'\b(sin\s+ning[uú]n\s+riesgo|riesgo\s+cero)\b',
 'improper_benefit': r'\b(beneficios?\s+de\s+por\s+vida\s+sin\s+restricciones)\b',
}

def detect(text: str, rules: dict[str,str] | None=None) -> list[str]:
    if not isinstance(text,str):return []
    return [name for name, regex in (rules or RULES).items()
            if re.search(regex,text,re.IGNORECASE)]

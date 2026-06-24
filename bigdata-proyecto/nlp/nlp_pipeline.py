"""
nlp_pipeline.py
================
Módulo NLP compartido entre jobs Flink y Spark.

Técnicas implementadas:
1. Tokenización y limpieza de texto (NLTK)
2. Detección de discurso de odio por diccionario (léxico contextualizado para Perú)
3. Análisis de sentimiento (TextBlob + reglas heurísticas en español)
4. Detección de "terruqueo" (acusación de terrorismo como arma política)
5. Clasificación de polarización política (keywords + TF-IDF scoring)
6. Detección de lenguaje discriminatorio (racial, género, clase social)
"""

import re
import os
import json
import unicodedata
from typing import Dict, List, Tuple


def _strip_accents(s: str) -> str:
    """Quita tildes/diacríticos (selváticos->selvaticos) para tolerar texto sin acentos."""
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")

# -------------------------------------------------------------------
# Listas de palabras (se cargan desde archivos en producción)
# -------------------------------------------------------------------

# NOTA: el matching es por PALABRA COMPLETA (límites \b), no por subcadena, para evitar
# falsos positivos (p.ej. "llamas"→llama, "colocar"→loca). Se excluyeron términos demasiado
# ambiguos en lenguaje cotidiano ("llama", "loca", "rojo", "izquierdo", "marginal").
HATE_WORDS = [
    # Racismo y discriminación étnica
    "cholo", "cholito", "indio", "serrano", "selvático", "zambito",
    "negrito", "asiático de mierda", "awelo",
    # Terruqueo (acusar de terrorismo sin evidencia)
    "terruco", "terrucos", "comunista", "caviares", "caviar",
    "marxista", "chavista", "senderista",
    # Misoginia política
    "feminazi", "hembrista",
    # Clasismo
    "chusma", "huachafo", "pituco de mierda",
]

TERRUCO_PATTERNS = [
    r"\bterruc[oa]s?\b",
    r"\bsenderi[sz]ta\b",
    r"\bcomu?ni[sz]ta\b",
    r"\bcaviar\b",
    r"\bhumal[ai]sta\b",
    r"\bcastrista\b",
    r"\bnarcoterror\b",
]

POLITICAL_POLARIZATION_WORDS = {
    "izquierda": ["comunismo", "socialismo", "colectivismo", "expropiar", "estatizar",
                  "sindicato", "huelga", "obrero", "Castillo", "Perú Libre"],
    "derecha":   ["empresa", "libre mercado", "inversión", "privatizar", "orden",
                  "fujimorismo", "Fujimori", "PPC", "Renovación Popular", "Fuerza Popular"],
}

DISCRIMINATION_CATEGORIES = {
    "racial":   ["cholo", "indio", "serrano", "zambo", "negro", "chino"],
    "genero":   ["feminazi", "bruja", "hembrista", "machista"],
    "clase":    ["chusma", "marginal", "huachafo", "pituco"],
    "regional": ["serrano", "selvático", "limeño de mierda", "provinciano"],
}


# -------------------------------------------------------------------
# Funciones principales
# -------------------------------------------------------------------

def clean_text(text: str) -> str:
    """Limpia el texto: minúsculas, sin URLs, sin caracteres especiales."""
    text = text.lower()
    text = re.sub(r"http\S+|www\S+", "", text)          # URLs
    text = re.sub(r"@\w+", "", text)                     # Menciones
    text = re.sub(r"#(\w+)", r"\1", text)                # Hashtags → palabra
    text = re.sub(r"[^\w\sáéíóúüñ]", " ", text)         # Caracteres especiales
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _find_terms(terms, cleaned: str) -> List[str]:
    """
    Busca cada término como PALABRA COMPLETA (límites \\b), no como subcadena.
    Evita falsos positivos como 'llamas'->llama o 'colocar'->loca.
    """
    cleaned_na = _strip_accents(cleaned)
    found = []
    for w in terms:
        # palabra completa + plural opcional, sin distinguir acentos (cholo->cholos, selvatico)
        pat = r"\b" + re.escape(_strip_accents(w.lower())) + r"(?:es|s)?\b"
        if re.search(pat, cleaned_na):
            found.append(w)
    return found


def detect_hate_speech(text: str) -> Tuple[bool, List[str]]:
    """
    Técnica 1: Detección de discurso de odio por léxico (palabra completa).
    Retorna (es_odio, palabras_encontradas).
    """
    cleaned = clean_text(text)
    found = _find_terms(HATE_WORDS, cleaned)
    return (len(found) > 0, found)


def detect_terruco(text: str) -> Tuple[bool, List[str]]:
    """
    Técnica 2: Detección de 'terruqueo' mediante expresiones regulares.
    El terruqueo es un fenómeno político peruano de acusar a opositores
    de ser terroristas sin evidencia.
    """
    cleaned = clean_text(text)
    matches = []
    for pattern in TERRUCO_PATTERNS:
        found = re.findall(pattern, cleaned)
        matches.extend(found)
    return (len(matches) > 0, matches)


def analyze_sentiment(text: str) -> Dict:
    """
    Técnica 3: Análisis de sentimiento basado en léxico (sin dependencia de ML pesado).
    Retorna score entre -1 (muy negativo) y 1 (muy positivo).
    """
    POSITIVE_WORDS = ["bueno", "bien", "excelente", "mejor", "progreso", "esperanza",
                      "paz", "desarrollo", "correcto", "apoyo", "gracias"]
    NEGATIVE_WORDS = ["malo", "mal", "terrible", "corrupto", "ladron", "mentira",
                      "asco", "odio", "mierda", "desgracia", "fracaso", "robo"]
    NEGATIONS = ["no", "nunca", "jamás", "ningún", "nada"]

    tokens = clean_text(text).split()
    score = 0
    i = 0
    while i < len(tokens):
        multiplier = -1 if (i > 0 and tokens[i-1] in NEGATIONS) else 1
        if tokens[i] in POSITIVE_WORDS:
            score += 1 * multiplier
        elif tokens[i] in NEGATIVE_WORDS:
            score -= 1 * multiplier
        i += 1

    # Normalizar
    word_count = max(len(tokens), 1)
    normalized = max(-1, min(1, score / (word_count * 0.5)))

    label = "positivo" if normalized > 0.1 else ("negativo" if normalized < -0.1 else "neutro")
    return {"score": round(normalized, 3), "label": label}


def classify_political_polarization(text: str) -> Dict:
    """
    Técnica 4: Clasificación de polarización política izquierda/derecha/neutro.
    Usa conteo de keywords ponderado.
    """
    cleaned = clean_text(text)
    scores = {"izquierda": 0, "derecha": 0}

    for side, keywords in POLITICAL_POLARIZATION_WORDS.items():
        scores[side] += len(_find_terms(keywords, cleaned))

    total = scores["izquierda"] + scores["derecha"]
    if total == 0:
        return {"label": "neutro", "scores": scores, "polarization_index": 0.0}

    # Índice de polarización: 0 = centrado, 1 = extremo
    diff = abs(scores["izquierda"] - scores["derecha"])
    polarization_index = round(diff / total, 3)

    label = max(scores, key=scores.get) if total > 0 else "neutro"
    return {
        "label": label,
        "scores": scores,
        "polarization_index": polarization_index
    }


def classify_discrimination_type(text: str) -> List[str]:
    """
    Técnica 5: Clasificación del tipo de discriminación detectada.
    Retorna lista de categorías encontradas.
    """
    cleaned = clean_text(text)
    found_categories = []
    for category, words in DISCRIMINATION_CATEGORIES.items():
        if _find_terms(words, cleaned):
            found_categories.append(category)
    return found_categories


def full_analysis(raw_text: str, source: str = "unknown", post_id: str = "") -> Dict:
    """
    Pipeline completo: aplica todas las técnicas y retorna un dict estructurado.
    Entrada: texto crudo
    Salida: JSON con todas las clasificaciones
    """
    is_hate, hate_words = detect_hate_speech(raw_text)
    is_terruco, terruco_matches = detect_terruco(raw_text)
    sentiment = analyze_sentiment(raw_text)
    political = classify_political_polarization(raw_text)
    discrim_types = classify_discrimination_type(raw_text)

    # Score de toxicidad compuesto
    toxicity_score = round(
        (0.4 * int(is_hate)) +
        (0.3 * int(is_terruco)) +
        (0.2 * len(discrim_types) / max(len(DISCRIMINATION_CATEGORIES), 1)) +
        (0.1 * max(0, -sentiment["score"])),
        3
    )

    return {
        "post_id": post_id,
        "source": source,
        "text_preview": raw_text[:100],
        "is_hate_speech": is_hate,
        "hate_words": hate_words,
        "is_terruco": is_terruco,
        "terruco_matches": terruco_matches,
        "sentiment": sentiment,
        "political_classification": political,
        "discrimination_types": discrim_types,
        "toxicity_score": toxicity_score,
        "needs_review": toxicity_score > 0.5,
    }


if __name__ == "__main__":
    # Test rápido
    test_texts = [
        "Este serrano comunista quiere expropiar todo, terruco de mierda",
        "Apoyamos el libre mercado y la inversión privada para el Perú",
        "Gracias a todos por el esfuerzo, trabajemos juntos por el país",
        "Los caviares defienden a los terroristas, no al pueblo peruano",
    ]
    for t in test_texts:
        result = full_analysis(t)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("---")

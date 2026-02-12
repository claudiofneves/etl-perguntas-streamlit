from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd


@dataclass
class ETLPaths:
    raw_csv: Path
    curated_csv: Path


REQUIRED_COLUMNS = [
    "timestamp",
    "turma",
    "aluno",
    "bloco",
    "question_id",
    "pergunta",
    "tipo",
    "resposta_aluno",
    "gabarito",
    "acertou"
]


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def extract(raw_csv: Path) -> pd.DataFrame:
    if not raw_csv.exists():
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    df = pd.read_csv(raw_csv, dtype="string", keep_default_na=False)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df[REQUIRED_COLUMNS]


def transform(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = df_raw.copy()

    # limpeza básica
    for col in ["turma", "aluno", "bloco", "question_id", "pergunta", "tipo", "resposta_aluno", "gabarito"]:
        df[col] = df[col].astype("string").fillna("").str.strip()

    # acertou -> int
    df["acertou"] = pd.to_numeric(df["acertou"], errors="coerce").fillna(0).astype("int64")

    # remove linhas sem aluno
    df = df[df["aluno"] != ""].copy()

    # dedup: última resposta do aluno naquela questão
    before = df.shape[0]
    df = df.drop_duplicates(subset=["aluno", "question_id"], keep="last")
    after = df.shape[0]

    metrics = {
        "linhas_raw": int(before),
        "linhas_curated": int(after),
        "duplicados_removidos": int(before - after),
        "total_acertos": int(df["acertou"].sum())
    }

    # ordenação
    df = df.sort_values(by=["turma", "aluno", "bloco", "question_id"])

    return df, metrics


def load(df_curated: pd.DataFrame, curated_csv: Path) -> None:
    ensure_parent_dir(curated_csv)
    df_curated.to_csv(curated_csv, index=False)


def run_pipeline(paths: ETLPaths) -> tuple[pd.DataFrame, dict]:
    df_raw = extract(paths.raw_csv)
    df_curated, metrics = transform(df_raw)
    load(df_curated, paths.curated_csv)
    return df_curated, metrics


def append_raw_row(paths: ETLPaths, row: dict) -> None:
    ensure_parent_dir(paths.raw_csv)

    df_existing = extract(paths.raw_csv)
    df_new = pd.DataFrame([row], columns=REQUIRED_COLUMNS)

    df_out = pd.concat([df_existing, df_new], ignore_index=True)
    df_out.to_csv(paths.raw_csv, index=False)

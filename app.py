from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st

from etl.pipeline import ETLPaths, append_raw_row, run_pipeline

# =========================
# Paths / Config
# =========================
BASE_DIR = Path(__file__).parent
QUESTIONS_PATH = BASE_DIR / "config" / "perguntas.json"
RAW_CSV = BASE_DIR / "data" / "raw" / "respostas_raw.csv"
CURATED_CSV = BASE_DIR / "data" / "curated" / "respostas_curadas.csv"

PATHS = ETLPaths(raw_csv=RAW_CSV, curated_csv=CURATED_CSV)


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@st.cache_data
def load_questions() -> dict:
    with open(QUESTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_processed() -> pd.DataFrame:
    """
    Lê a base tratada (resultado do ETL).
    """
    if CURATED_CSV.exists():
        df = pd.read_csv(CURATED_CSV, dtype="string", keep_default_na=False)
        if "acertou" in df.columns:
            df["acertou"] = pd.to_numeric(df["acertou"], errors="coerce").fillna(0).astype("int64")
        return df
    return pd.DataFrame()


# =========================
# UI
# =========================
st.set_page_config(page_title="Quiz ETL (Acertos)", layout="wide")

# =========================
# Cabeçalho Institucional
# =========================
st.markdown(
    """
    <div style="
        background-color:#f2f4f7;
        padding:20px;
        border-radius:10px;
        border: 1px solid #d0d7de;
    ">
        <h2 style="margin-bottom:5px;">
            Aula Prática:
            Desenvolvimento de pipeline para ETL/ELT com Python
        </h2>
        <p style="margin:0; font-size:16px;">
            <strong>Data:</strong> 12/02/2026 às 13h
        </p>
        <p style="margin:0; font-size:16px;">
            <strong>Professor:</strong> Cláudio Ferreira Neves
        </p>
        <p style="margin:0; font-size:16px;">
            <strong>Cargo:</strong> Mentor Educacional de TI - Horista - Analista de Dados e Visualização
        </p>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("---")

st.title("Quiz ETL — Perguntas Objetivas + Feedback + Resultados")
st.caption("Fluxo: respostas chegam como **dados brutos** → ETL trata e consolida → painel usa a **base tratada**.")

tabs = st.tabs(["📝 Responder", "⚙️ Rodar ETL", "📊 Resultados"])
questions_by_block = load_questions()

# -------------------------
# Tab: Responder
# -------------------------
with tabs[0]:
    col1, col2, col3 = st.columns(3)
    with col1:
        turma = st.text_input("Turma", value="Turma A")
    with col2:
        aluno = st.text_input("Aluno (obrigatório)", value="")
    with col3:
        bloco = st.selectbox("Bloco", list(questions_by_block.keys()))

    st.divider()

    st.info(
        "📌 **Como funciona (ETL na prática):**\n"
        "- Ao enviar, suas respostas são gravadas na **Base Bruta (RAW)**.\n"
        "- Depois, o professor roda o **ETL** para **consolidar e tratar** (ex.: manter a última resposta por questão).\n"
        "- O painel usa a **Base Tratada** (resultado do ETL) para calcular acertos e rankings."
    )

    st.divider()

    q_list = questions_by_block[bloco]
    mostrar_feedback = st.toggle("Mostrar feedback (correta + explicação) após enviar", value=True)

    with st.form("form_quiz"):
        answers: list[tuple[dict, str | None]] = []

        for q in q_list:
            st.markdown(f"**{q['pergunta']}**")
            op_keys = list(q["opcoes"].keys())

            # ✅ Sem marcação automática
            choice = st.radio(
                label="Escolha uma alternativa:",
                options=op_keys,
                format_func=lambda k, _q=q: f"{k} — {_q['opcoes'][k]}",
                index=None,
                key=q["id"],
            )

            answers.append((q, choice))
            st.write("")

        submitted = st.form_submit_button("✅ Enviar respostas")

    if submitted:
        if aluno.strip() == "":
            st.error("Informe o nome do aluno.")
            st.stop()

        if any(choice is None for _, choice in answers):
            st.error("Responda todas as questões (marque uma alternativa em cada uma).")
            st.stop()

        feedback = []
        total_acertos = 0

        for q, choice in answers:
            acertou = 1 if choice == q["gabarito"] else 0
            total_acertos += acertou

            row = {
                "timestamp": utc_now_iso(),
                "turma": turma.strip(),
                "aluno": aluno.strip(),
                "bloco": bloco.strip(),
                "question_id": q["id"],
                "pergunta": q["pergunta"],
                "tipo": q.get("tipo", "multipla"),
                "resposta_aluno": choice,
                "gabarito": q["gabarito"],
                "acertou": acertou,
            }
            append_raw_row(PATHS, row)

            feedback.append((q, choice, acertou))

        st.success("Respostas registradas na **Base Bruta (RAW)**.")
        st.info("Para atualizar o painel geral (gráficos e ranking), vá na aba **Rodar ETL**.")

        if mostrar_feedback:
            st.divider()
            st.subheader("Feedback das respostas")
            st.write(f"**Resumo do bloco:** {total_acertos} acerto(s) de {len(feedback)} questão(ões).")

            for q, choice, acertou in feedback:
                correta = q["gabarito"]
                texto_correta = q["opcoes"][correta]
                explicacao = q.get("explicacao", "").strip()

                texto_marcada = q["opcoes"].get(choice, "")

                if acertou == 1:
                    st.success(
                        f"✅ **{q['pergunta']}**\n\n"
                        f"Você marcou: **{choice} — {texto_marcada}**\n\n"
                        f"Correto."
                    )
                else:
                    st.error(
                        f"❌ **{q['pergunta']}**\n\n"
                        f"Você marcou: **{choice} — {texto_marcada}**\n\n"
                        f"✅ Correta: **{correta} — {texto_correta}**"
                    )
                    if explicacao:
                        st.info(f"🧠 **Por que é a correta?** {explicacao}")
                    else:
                        st.info("🧠 **Por que é a correta?** (adicione o campo `explicacao` no perguntas.json)")

# -------------------------
# Tab: Rodar ETL
# -------------------------
with tabs[1]:
    st.subheader("Rodar ETL (Base Bruta → Base Tratada)")

    st.write(
        "Aqui está o motivo do ETL existir neste projeto:\n\n"
        "- A **Base Bruta (RAW)** é o que chega primeiro (pode ter duplicidade e respostas repetidas).\n"
        "- O **ETL** consolida e trata os dados (ex.: mantém a **última resposta do aluno por questão**).\n"
        "- O resultado vira a **Base Tratada**, usada para gráficos e ranking com dados confiáveis."
    )

    if st.button("▶️ Rodar ETL agora"):
        df_processed, metrics = run_pipeline(PATHS)
        st.success("ETL executado. Painel atualizado com a **Base Tratada**.")
        st.json(metrics)

# -------------------------
# Tab: Resultados
# -------------------------
with tabs[2]:
    st.subheader("Resultados (Base Tratada)")

    df = load_processed()

    if df.empty:
        st.warning("Ainda não existe **Base Tratada**. Rode o ETL na aba **Rodar ETL**.")
    else:
        df["acertou"] = pd.to_numeric(df["acertou"], errors="coerce").fillna(0).astype("int64")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Respostas (consolidadas)", int(df.shape[0]))
        c2.metric("Alunos", int(df["aluno"].nunique()))
        c3.metric("Acertos", int(df["acertou"].sum()))
        total = int(df.shape[0]) if df.shape[0] > 0 else 1
        c4.metric("% Acerto", f"{(df['acertou'].sum() / total) * 100:.1f}%")

        st.divider()

        # ✅ GRÁFICO COM EIXO X = ALUNO
        st.markdown("### Acertos por aluno (Eixo X = Aluno)")
        by_aluno = (
            df.groupby("aluno")["acertou"]
            .sum()
            .reset_index()
            .sort_values("acertou", ascending=False)
        )
        st.bar_chart(by_aluno.set_index("aluno")["acertou"])

        st.divider()

        st.markdown("### Ranking de alunos (acertos)")
        ranking = (
            df.groupby("aluno")["acertou"]
            .sum()
            .reset_index()
            .sort_values("acertou", ascending=False)
        )
        st.dataframe(ranking, use_container_width=True)

        st.divider()

        st.markdown("### Base Tratada (dados finais para análise)")
        st.dataframe(df, use_container_width=True)

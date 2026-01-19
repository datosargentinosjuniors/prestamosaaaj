# app.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import uuid
from io import BytesIO
import matplotlib.pyplot as plt

# =========================
# Configuración Streamlit
# =========================
st.set_page_config(page_title="Seguimiento de Préstamos", layout="wide")
st.title("📌 Seguimiento de Préstamos - Secretaría Técnica")

# =========================
# Google Sheets: conexión
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_NAME = st.secrets.get("GSPREAD_SHEET_NAME", "")

PUESTOS = [
    "Arquero",
    "Defensor central",
    "Lateral",
    "Mediocampista defensivo",
    "Mediocampista mixto",
    "Mediocampista ofensivo",
    "Extremo",
    "Delantero",
]

DIVISIONES = ["1° división", "2° división", "3° división"]

# =========================
# Nombres institucionales (solo visualización)
# =========================
DISPLAY_LABELS = {
    # Jugadores
    "jugador_id": "ID jugador",
    "nombre": "Nombre",
    "puesto": "Puesto",
    "fecha_nacimiento": "Fecha de nacimiento",
    "pais_prestamo": "País",
    "division_prestamo": "División",
    "club_prestamo": "Club",
    "opcion_compra": "Opción de compra",
    "posibilidad_repesca": "Posibilidad de repesca",
    "fecha_retorno": "Fecha de retorno",
    "fin_contrato_aaaj": "Fin de contrato AAAJ",
    "estado": "Estado",
    "observaciones": "Observaciones",
    # Seguimiento
    "registro_id": "ID registro",
    "week_start": "Semana (inicio)",
    "week_end": "Semana (fin)",
    "partidos": "Partidos",
    "minutos": "Minutos",
    "goles_marcados": "Goles",
    "goles_encajados": "Goles encajados",
    "amarillas": "Amarillas",
    "rojas": "Rojas",
    "incidencias": "Incidencias",
    # Reportes
    "reporte_id": "ID reporte",
    "titulo": "Título",
    "fecha_reporte": "Fecha del reporte",
    "fecha_creacion": "Creado (fecha y hora)",
    "contenido": "Reporte",
    # Acumulados
    "partidos_total": "Partidos (total)",
    "minutos_total": "Minutos (total)",
    "goles_total": "Goles (total)",
    "encajados_total": "Goles encajados (total)",
    "amarillas_total": "Amarillas (total)",
    "rojas_total": "Rojas (total)",
    "ultima_semana": "Última semana",
}

# =========================
# Columnas base (Sheets)
# =========================
REQUIRED_JUGADORES_COLS = [
    "jugador_id",
    "nombre",
    "puesto",
    "fecha_nacimiento",
    "pais_prestamo",
    "division_prestamo",
    "club_prestamo",
    "opcion_compra",
    "posibilidad_repesca",
    "fecha_retorno",
    "fin_contrato_aaaj",
    "estado",
    "observaciones",
    "created_at",
    "updated_at",
]

REQUIRED_SEGUIMIENTO_COLS = [
    "registro_id",
    "jugador_id",
    "week_start",
    "week_end",
    "partidos",
    "minutos",
    "goles_marcados",
    "goles_encajados",
    "amarillas",
    "rojas",
    "incidencias",
    "created_at",
    "updated_at",
]

# ✅ Reportes (con título + fecha creación)
REQUIRED_REPORTES_COLS = [
    "reporte_id",
    "jugador_id",
    "titulo",
    "fecha_reporte",
    "fecha_creacion",
    "contenido",
    "created_at",
    "updated_at",
]

# =========================
# Utilidades
# =========================
def _parse_date_safe(x):
    if x is None:
        return pd.NaT
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    s = str(x).strip()
    if s == "" or s.lower() in ["nat", "none"]:
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        out = pd.to_datetime(s, errors="coerce")
        return out.date() if not pd.isna(out) else pd.NaT
    except Exception:
        return pd.NaT


def _parse_dt_safe(x):
    """Parse de datetime tolerante (para fecha_creacion)."""
    if x is None:
        return pd.NaT
    if isinstance(x, datetime):
        return x
    s = str(x).strip()
    if s == "" or s.lower() in ["nat", "none"]:
        return pd.NaT
    try:
        out = pd.to_datetime(s, errors="coerce")
        return out if not pd.isna(out) else pd.NaT
    except Exception:
        return pd.NaT


def hoy_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_week_start(d=None):
    """Lunes de la semana"""
    if d is None:
        d = date.today()
    if isinstance(d, datetime):
        d = d.date()
    return d - timedelta(days=d.weekday())


def get_week_end_from_start(week_start: date) -> date:
    """Domingo de esa semana (inicio + 6)"""
    return week_start + timedelta(days=6)


def is_gk(puesto: str) -> bool:
    if puesto is None:
        return False
    p = str(puesto).strip().lower()
    return "arquero" in p or p in ["gk", "goalkeeper", "portero"]


def pretty_df(df: pd.DataFrame, cols: list, hide_internal_ids: bool = False) -> pd.DataFrame:
    cols2 = cols.copy()
    if hide_internal_ids:
        cols2 = [c for c in cols2 if c not in ["jugador_id", "registro_id", "reporte_id"]]

    out = df.copy()
    for c in cols2:
        if c not in out.columns:
            out[c] = ""
    out = out[cols2].copy()
    out = out.rename(columns={c: DISPLAY_LABELS.get(c, c.replace("_", " ").title()) for c in cols2})
    return out


def kpi_card(title: str, value: str):
    st.markdown(
        f"""
        <div style="
            border:1px solid rgba(255,255,255,0.12);
            border-radius:12px;
            padding:10px 12px;
            background: rgba(255,255,255,0.03);
            ">
            <div style="font-size:12px; opacity:0.8; margin-bottom:6px;">{title}</div>
            <div style="font-size:16px; font-weight:700; line-height:1.2; word-break:break-word;">
                {value}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def barh_with_labels_weekrange(
    df: pd.DataFrame,
    week_start_col: str,
    week_end_col: str,
    value_col: str,
    title: str,
    title_size: int = 12,
    axis_label_size: int = 11,
    value_label_size: int = 10,
    bar_height_factor: float = 0.45
):
    if df.empty:
        st.info("No hay datos para graficar.")
        return

    dd = df.copy().sort_values(week_start_col)
    dd[week_start_col] = dd[week_start_col].apply(_parse_date_safe)
    dd[week_end_col] = dd[week_end_col].apply(_parse_date_safe)

    y_labels = [
        f"{ws.strftime('%d/%m/%Y')} → {we.strftime('%d/%m/%Y')}"
        for ws, we in zip(dd[week_start_col], dd[week_end_col])
    ]
    x_vals = pd.to_numeric(dd[value_col], errors="coerce").fillna(0).astype(int).tolist()

    fig_height = max(3.2, bar_height_factor * len(dd))
    fig, ax = plt.subplots(figsize=(10, fig_height))
    bars = ax.barh(y_labels, x_vals, color="#d60000")

    ax.set_title(title, fontsize=title_size, pad=10)
    ax.tick_params(axis="x", labelsize=axis_label_size)
    ax.tick_params(axis="y", labelsize=axis_label_size)

    for rect, val in zip(bars, x_vals):
        if val == 0:
            continue
        ax.text(
            rect.get_width() / 2,
            rect.get_y() + rect.get_height() / 2,
            f"{val}",
            ha="center",
            va="center",
            color="white",
            fontsize=value_label_size,
            fontweight="bold"
        )

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    fig.tight_layout()
    st.pyplot(fig)


# =========================
# Google Sheets client
# =========================
@st.cache_resource
def get_gspread_client():
    creds_info = dict(st.secrets["google_service_account"])
    credentials = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(credentials)


@st.cache_resource
def get_workbook():
    if not SHEET_NAME:
        st.error("Falta configurar GSPREAD_SHEET_NAME en secrets")
        st.stop()
    gc = get_gspread_client()
    try:
        sh = gc.open(SHEET_NAME)
    except Exception as e:
        st.error(
            "No pude abrir el Google Sheet. Revisá:\n"
            "- nombre de sheet (GSPREAD_SHEET_NAME)\n"
            "- share del sheet al service account\n"
            "- credenciales\n\n"
            f"Detalle: {e}"
        )
        st.stop()
    return sh


def _get_or_create_worksheet(sh, title, cols):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=1000, cols=max(20, len(cols)))
        ws.append_row(cols)
        return ws

    values = ws.get_all_values()
    if not values:
        ws.append_row(cols)
        return ws

    header = values[0]
    if header != cols:
        df_old = pd.DataFrame(values[1:], columns=header) if len(values) > 1 else pd.DataFrame(columns=header)
        for c in cols:
            if c not in df_old.columns:
                df_old[c] = ""
        df_new = df_old[cols].copy()
        ws.clear()
        ws.append_row(cols)
        if not df_new.empty:
            ws.append_rows(df_new.fillna("").astype(str).values.tolist())
    return ws


def init_sheets():
    sh = get_workbook()
    ws_j = _get_or_create_worksheet(sh, "jugadores", REQUIRED_JUGADORES_COLS)
    ws_s = _get_or_create_worksheet(sh, "seguimiento", REQUIRED_SEGUIMIENTO_COLS)
    ws_r = _get_or_create_worksheet(sh, "reportes", REQUIRED_REPORTES_COLS)
    return ws_j, ws_s, ws_r


def _ws_to_df(ws):
    values = ws.get_all_values()
    if not values or len(values) == 1:
        return pd.DataFrame(columns=values[0] if values else [])
    header = values[0]
    data = values[1:]
    return pd.DataFrame(data, columns=header)


def _df_to_ws_overwrite(ws, df, cols):
    df_out = df.copy()
    for c in cols:
        if c not in df_out.columns:
            df_out[c] = ""
    df_out = df_out[cols]
    ws.clear()
    ws.append_row(cols)
    if not df_out.empty:
        ws.append_rows(df_out.fillna("").astype(str).values.tolist())


@st.cache_data(ttl=30)
def load_data():
    ws_j, ws_s, ws_r = init_sheets()
    df_j = _ws_to_df(ws_j)
    df_s = _ws_to_df(ws_s)
    df_r = _ws_to_df(ws_r)
    return df_j, df_s, df_r


def save_jugadores(df_j):
    ws_j, _, _ = init_sheets()
    _df_to_ws_overwrite(ws_j, df_j, REQUIRED_JUGADORES_COLS)
    st.cache_data.clear()


def save_seguimiento(df_s):
    _, ws_s, _ = init_sheets()
    _df_to_ws_overwrite(ws_s, df_s, REQUIRED_SEGUIMIENTO_COLS)
    st.cache_data.clear()


def save_reportes(df_r):
    _, _, ws_r = init_sheets()
    _df_to_ws_overwrite(ws_r, df_r, REQUIRED_REPORTES_COLS)
    st.cache_data.clear()


# =========================
# Normalización
# =========================
def _coerce_bool_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str).str.strip().str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "si": True, "sí": True, "no": False})
        .fillna(False)
    )


def normalizar_jugadores(df_j):
    if df_j.empty:
        df_j = pd.DataFrame(columns=REQUIRED_JUGADORES_COLS)

    for col in ["fecha_nacimiento", "fecha_retorno", "fin_contrato_aaaj"]:
        if col in df_j.columns:
            df_j[col] = df_j[col].apply(_parse_date_safe)

    # booleanos
    if "opcion_compra" in df_j.columns:
        df_j["opcion_compra"] = _coerce_bool_series(df_j["opcion_compra"])
    if "posibilidad_repesca" in df_j.columns:
        df_j["posibilidad_repesca"] = _coerce_bool_series(df_j["posibilidad_repesca"])

    return df_j


def normalizar_seguimiento(df_s):
    if df_s.empty:
        df_s = pd.DataFrame(columns=REQUIRED_SEGUIMIENTO_COLS)

    for col in ["week_start", "week_end"]:
        if col in df_s.columns:
            df_s[col] = df_s[col].apply(_parse_date_safe)

    num_cols = ["partidos", "minutos", "goles_marcados", "goles_encajados", "amarillas", "rojas"]
    for c in num_cols:
        if c in df_s.columns:
            df_s[c] = pd.to_numeric(df_s[c], errors="coerce").fillna(0).astype(int)
    return df_s


def normalizar_reportes(df_r):
    if df_r.empty:
        df_r = pd.DataFrame(columns=REQUIRED_REPORTES_COLS)
    if "fecha_reporte" in df_r.columns:
        df_r["fecha_reporte"] = df_r["fecha_reporte"].apply(_parse_date_safe)
    if "fecha_creacion" in df_r.columns:
        df_r["fecha_creacion"] = df_r["fecha_creacion"].apply(_parse_dt_safe)
    return df_r


def upsert_jugador(df_j, jugador_id, payload: dict):
    df_new = df_j.copy()
    if (df_new["jugador_id"].astype(str) == str(jugador_id)).any():
        mask = df_new["jugador_id"].astype(str) == str(jugador_id)
        for k, v in payload.items():
            df_new.loc[mask, k] = v
        df_new.loc[mask, "updated_at"] = hoy_str()
    else:
        now = hoy_str()
        row = {c: "" for c in REQUIRED_JUGADORES_COLS}
        row.update(payload)
        row["jugador_id"] = str(jugador_id)
        row["created_at"] = now
        row["updated_at"] = now
        df_new = pd.concat([df_new, pd.DataFrame([row])], ignore_index=True)

    for c in REQUIRED_JUGADORES_COLS:
        if c not in df_new.columns:
            df_new[c] = ""
    return df_new[REQUIRED_JUGADORES_COLS]


def baja_jugador_soft(df_j: pd.DataFrame, jugador_id: str, motivo: str = "") -> pd.DataFrame:
    df_new = df_j.copy()
    mask = df_new["jugador_id"].astype(str) == str(jugador_id)
    if not mask.any():
        return df_new
    df_new.loc[mask, "estado"] = "Rescindido"
    if motivo.strip():
        obs_old = df_new.loc[mask, "observaciones"].astype(str).fillna("").values[0]
        add = f"[Baja] {motivo.strip()}"
        df_new.loc[mask, "observaciones"] = (obs_old + "\n" + add).strip() if obs_old else add
    df_new.loc[mask, "updated_at"] = hoy_str()
    return df_new


def eliminar_jugador_hard(df_j: pd.DataFrame, df_s: pd.DataFrame, df_r: pd.DataFrame, jugador_id: str):
    dfj = df_j[df_j["jugador_id"].astype(str) != str(jugador_id)].copy()
    dfs = df_s[df_s["jugador_id"].astype(str) != str(jugador_id)].copy()
    dfr = df_r[df_r["jugador_id"].astype(str) != str(jugador_id)].copy()
    return dfj, dfs, dfr


# =========================
# Export Excel (XLSX)
# =========================
def make_excel_bytes(df_j: pd.DataFrame, df_s: pd.DataFrame, df_r: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_j_export = df_j.copy()
        df_s_export = df_s.copy()
        df_r_export = df_r.copy()

        df_j_export.to_excel(writer, index=False, sheet_name="Jugadores")
        df_s_export.to_excel(writer, index=False, sheet_name="Seguimiento")
        df_r_export.to_excel(writer, index=False, sheet_name="Reportes")

        try:
            cols_j_view = [
                "nombre", "puesto", "fecha_nacimiento",
                "pais_prestamo", "division_prestamo", "club_prestamo",
                "estado", "opcion_compra", "posibilidad_repesca",
                "fecha_retorno", "fin_contrato_aaaj",
                "observaciones", "jugador_id"
            ]
            pretty_df(df_j_export, cols_j_view, hide_internal_ids=False).to_excel(
                writer, index=False, sheet_name="Jugadores (Vista)"
            )

            cols_s_view = [
                "week_start", "week_end",
                "partidos", "minutos", "goles_marcados", "goles_encajados",
                "amarillas", "rojas", "incidencias",
                "jugador_id", "registro_id"
            ]
            pretty_df(df_s_export, cols_s_view, hide_internal_ids=False).to_excel(
                writer, index=False, sheet_name="Seguimiento (Vista)"
            )

            cols_r_view = ["titulo", "fecha_reporte", "fecha_creacion", "contenido", "jugador_id", "reporte_id"]
            pretty_df(df_r_export, cols_r_view, hide_internal_ids=False).to_excel(
                writer, index=False, sheet_name="Reportes (Vista)"
            )
        except Exception:
            pass

    return output.getvalue()


# =========================
# UI: Navegación
# =========================
pages = [
    "➕ Alta / Edición de Jugadores",
    "🗓️ Carga Semanal",
    "📊 Tabla Acumulada",
    "👤 Vista Individual",
    "📝 Reportes",
    "⚙️ Administración / Export",
]
page = st.sidebar.radio("Navegación", pages)

df_j_raw, df_s_raw, df_r_raw = load_data()
df_j = normalizar_jugadores(df_j_raw)
df_s = normalizar_seguimiento(df_s_raw)
df_r = normalizar_reportes(df_r_raw)

for c in REQUIRED_JUGADORES_COLS:
    if c not in df_j.columns:
        df_j[c] = ""
for c in REQUIRED_SEGUIMIENTO_COLS:
    if c not in df_s.columns:
        df_s[c] = ""
for c in REQUIRED_REPORTES_COLS:
    if c not in df_r.columns:
        df_r[c] = ""

# =========================
# Página 1: Jugadores
# =========================
if page == pages[0]:
    st.subheader("➕ Alta / Edición de Jugadores")

    tab_crear, tab_editar = st.tabs(["Crear jugador", "Editar jugador"])

    with tab_crear:
        st.markdown("### Crear jugador")
        with st.form("form_crear_jugador", clear_on_submit=True):
            nombre = st.text_input("Nombre", placeholder="Escribe nombre del jugador.")
            puesto = st.selectbox("Puesto", PUESTOS)

            fecha_nac = st.date_input(
                "Fecha de nacimiento",
                value=date.today(),
                min_value=date(1950, 1, 1),
                max_value=date.today()
            )

            pais = st.text_input("País (préstamo)", placeholder="País en donde jugará el jugador.")
            division = st.selectbox("División", DIVISIONES, index=0)
            club_prestamo = st.text_input("Club (préstamo)", placeholder="Escribe el club en el que está a prestamo.")

            opcion_compra = st.checkbox("¿Tiene opción de compra?")
            posibilidad_repesca = st.checkbox("¿Tiene posibilidad de repesca?")
            fecha_retorno = st.date_input("Fecha de retorno", value=date.today() + relativedelta(months=6))
            fin_contrato = st.date_input("Fin de contrato con AAAJ", value=date.today() + relativedelta(years=2))
            estado = st.selectbox("Estado", ["Activo", "Finalizado", "Rescindido"])
            obs = st.text_area("Observaciones", placeholder="Notas relevantes...")

            submitted = st.form_submit_button("✅ Guardar jugador")
            if submitted:
                if not nombre.strip():
                    st.error("El nombre no puede estar vacío.")
                else:
                    jugador_id = str(uuid.uuid4())
                    payload = {
                        "nombre": nombre.strip(),
                        "puesto": puesto,
                        "fecha_nacimiento": fecha_nac,
                        "pais_prestamo": pais.strip(),
                        "division_prestamo": division,
                        "club_prestamo": club_prestamo.strip(),
                        "opcion_compra": bool(opcion_compra),
                        "posibilidad_repesca": bool(posibilidad_repesca),
                        "fecha_retorno": fecha_retorno,
                        "fin_contrato_aaaj": fin_contrato,
                        "estado": estado,
                        "observaciones": obs.strip(),
                    }
                    df_new = upsert_jugador(df_j, jugador_id, payload)
                    save_jugadores(df_new)
                    st.success("Jugador guardado ✅")

    with tab_editar:
        st.markdown("### Editar jugador")

        if df_j.empty:
            st.info("Todavía no hay jugadores cargados.")
        else:
            dfj2 = normalizar_jugadores(df_j.copy())
            dfj2["label"] = (
                dfj2["nombre"].astype(str)
                + " — "
                + dfj2["club_prestamo"].astype(str)
                + " ("
                + dfj2["puesto"].astype(str)
                + ")"
            )
            label_to_id = dict(zip(dfj2["label"], dfj2["jugador_id"]))

            sel = st.selectbox("Seleccionar jugador", dfj2["label"].tolist())
            jugador_id = label_to_id[sel]
            j = dfj2[dfj2["jugador_id"].astype(str) == str(jugador_id)].iloc[0]

            def _date_or_default(x, default):
                return x if isinstance(x, date) else default

            with st.form("form_editar_jugador", clear_on_submit=False):
                nombre = st.text_input("Nombre", value=str(j.get("nombre", "")))
                puesto_val = str(j.get("puesto", PUESTOS[0]))
                puesto = st.selectbox(
                    "Puesto", PUESTOS,
                    index=PUESTOS.index(puesto_val) if puesto_val in PUESTOS else 0
                )

                fecha_nac = st.date_input(
                    "Fecha de nacimiento",
                    value=_date_or_default(j.get("fecha_nacimiento", pd.NaT), date(2000, 1, 1)),
                    min_value=date(1950, 1, 1),
                    max_value=date.today()
                )

                pais = st.text_input("País (préstamo)", value=str(j.get("pais_prestamo", "")))
                div_val = str(j.get("division_prestamo", DIVISIONES[0]))
                division = st.selectbox(
                    "División", DIVISIONES,
                    index=DIVISIONES.index(div_val) if div_val in DIVISIONES else 0
                )
                club_prestamo = st.text_input("Club (préstamo)", value=str(j.get("club_prestamo", "")))

                opcion_compra = st.checkbox("Tiene opción de compra", value=bool(j.get("opcion_compra", False)))
                posibilidad_repesca = st.checkbox("Tiene posibilidad de repesca", value=bool(j.get("posibilidad_repesca", False)))

                fecha_retorno = st.date_input(
                    "Fecha de retorno",
                    value=_date_or_default(j.get("fecha_retorno", pd.NaT), date.today() + relativedelta(months=6))
                )
                fin_contrato = st.date_input(
                    "Fin de contrato con AAAJ",
                    value=_date_or_default(j.get("fin_contrato_aaaj", pd.NaT), date.today() + relativedelta(years=2))
                )
                estado_val = str(j.get("estado", "Activo"))
                estado = st.selectbox(
                    "Estado", ["Activo", "Finalizado", "Rescindido"],
                    index=["Activo", "Finalizado", "Rescindido"].index(estado_val) if estado_val in ["Activo", "Finalizado", "Rescindido"] else 0
                )
                obs = st.text_area("Observaciones", value=str(j.get("observaciones", "")))

                submitted = st.form_submit_button("✅ Guardar cambios")
                if submitted:
                    if not nombre.strip():
                        st.error("El nombre no puede estar vacío.")
                    else:
                        payload = {
                            "nombre": nombre.strip(),
                            "puesto": puesto,
                            "fecha_nacimiento": fecha_nac,
                            "pais_prestamo": pais.strip(),
                            "division_prestamo": division,
                            "club_prestamo": club_prestamo.strip(),
                            "opcion_compra": bool(opcion_compra),
                            "posibilidad_repesca": bool(posibilidad_repesca),
                            "fecha_retorno": fecha_retorno,
                            "fin_contrato_aaaj": fin_contrato,
                            "estado": estado,
                            "observaciones": obs.strip(),
                        }
                        df_new = upsert_jugador(df_j, jugador_id, payload)
                        save_jugadores(df_new)
                        st.success("Cambios guardados ✅")

            st.markdown("---")
            st.markdown("### Acciones sobre el jugador")

            col1, col2 = st.columns([1, 1], gap="large")

            with col1:
                st.markdown("**Dar de baja (recomendado)**")
                motivo = st.text_input(
                    "Motivo / nota de baja",
                    key="motivo_baja",
                    placeholder="Ej: Fin de préstamo / rescisión / etc."
                )
                if st.button("🚫 Dar de baja (Rescindido)", type="secondary"):
                    df_new = baja_jugador_soft(df_j, jugador_id, motivo=motivo)
                    save_jugadores(df_new)
                    st.success("Jugador dado de baja (estado: Rescindido) ✅")

            with col2:
                st.markdown("**Eliminar definitivamente**")
                st.caption("Esto borra al jugador, su seguimiento y sus reportes.")
                confirm_text = st.text_input(
                    "Escribí ELIMINAR para confirmar",
                    key="confirm_eliminar",
                    placeholder="ELIMINAR",
                )
                if st.button(
                    "🗑️ Eliminar definitivamente",
                    type="primary",
                    disabled=(confirm_text.strip().upper() != "ELIMINAR")
                ):
                    dfj_new, dfs_new, dfr_new = eliminar_jugador_hard(df_j, df_s, df_r, jugador_id)
                    save_jugadores(dfj_new)
                    save_seguimiento(dfs_new)
                    save_reportes(dfr_new)
                    st.success("Jugador, seguimiento y reportes eliminados ✅")
                    st.info("Actualizá la página o elegí otro jugador en el selector.")

    st.markdown("---")
    st.markdown("### Tabla de jugadores")
    st.caption("Esta tabla es solo para ver/filtrar. Para editar, usá el tab de **Editar jugador** (arriba).")

    df_view = normalizar_jugadores(df_j.copy())
    if df_view.empty:
        st.info("No hay jugadores cargados todavía.")
    else:
        show_cols = [
            "nombre", "puesto",
            "pais_prestamo", "division_prestamo", "club_prestamo",
            "estado", "opcion_compra", "posibilidad_repesca",
            "fecha_retorno", "fin_contrato_aaaj",
            "observaciones", "jugador_id"
        ]
        st.dataframe(
            pretty_df(df_view, show_cols, hide_internal_ids=False),
            use_container_width=True,
            hide_index=True
        )

# =========================
# Página 2: Carga semanal
# =========================
elif page == pages[1]:
    st.subheader("🗓️ Carga Semanal")

    activos = df_j[df_j["estado"].astype(str).str.lower().eq("activo")].copy()
    if activos.empty:
        st.warning("No hay jugadores activos cargados. Andá a 'Alta / Edición de Jugadores'.")
        st.stop()

    activos["label"] = (
        activos["nombre"].astype(str)
        + " — "
        + activos["club_prestamo"].astype(str)
        + " ("
        + activos["puesto"].astype(str)
        + ")"
    )
    label_to_id = dict(zip(activos["label"], activos["jugador_id"]))

    st.markdown("### Cargar semana")

    jugador_label = st.selectbox("Jugador", activos["label"].tolist())
    jugador_id = label_to_id[jugador_label]
    jugador_row = activos.loc[activos["jugador_id"] == jugador_id].iloc[0]
    puesto = str(jugador_row["puesto"])

    with st.form("form_carga_semanal", clear_on_submit=True):
        week_start = st.date_input(
            "Semana (inicio - Lunes)",
            value=get_week_start(),
            help="Elegí el Lunes de la semana a considerar. El sistema calcula automáticamente el fin (Domingo)."
        )
        week_end = get_week_end_from_start(week_start)
        st.caption(f"Semana seleccionada: **{week_start.strftime('%d/%m/%Y')}** → **{week_end.strftime('%d/%m/%Y')}**")

        partidos = st.number_input("Partidos", min_value=0, max_value=10, value=0, step=1)
        minutos = st.number_input("Minutos", min_value=0, max_value=900, value=0, step=1)

        if is_gk(puesto):
            goles_encajados = st.number_input("Goles encajados", min_value=0, max_value=50, value=0, step=1)
            goles_marcados = 0
        else:
            goles_marcados = st.number_input("Goles", min_value=0, max_value=50, value=0, step=1)
            goles_encajados = 0

        amarillas = st.number_input("Amarillas", min_value=0, max_value=10, value=0, step=1)
        rojas = st.number_input("Rojas", min_value=0, max_value=10, value=0, step=1)
        incidencias = st.text_area("Incidencias / notas", placeholder="Lesión, debut, asistencia, expulsión, etc.")

        submit = st.form_submit_button("Guardar semana")
        if submit:
            exists = (
                (df_s["jugador_id"].astype(str) == str(jugador_id)) &
                (df_s.get("week_start", pd.Series([None] * len(df_s))).apply(_parse_date_safe) == week_start)
            )
            if exists.any():
                st.error("Ya existe un registro para ese jugador en esa semana. Corregilo desde Admin/Export.")
            else:
                now = hoy_str()
                new_row = {
                    "registro_id": str(uuid.uuid4()),
                    "jugador_id": str(jugador_id),
                    "week_start": week_start,
                    "week_end": week_end,
                    "partidos": int(partidos),
                    "minutos": int(minutos),
                    "goles_marcados": int(goles_marcados),
                    "goles_encajados": int(goles_encajados),
                    "amarillas": int(amarillas),
                    "rojas": int(rojas),
                    "incidencias": incidencias.strip(),
                    "created_at": now,
                    "updated_at": now,
                }
                df_s2 = pd.concat([df_s, pd.DataFrame([new_row])], ignore_index=True)
                save_seguimiento(df_s2)
                st.success("Carga guardada ✅")

    st.markdown("---")
    st.markdown("### Últimos registros del jugador")

    df_player = df_s[df_s["jugador_id"].astype(str) == str(jugador_id)].copy()
    if df_player.empty:
        st.info("Aún no hay cargas para este jugador.")
    else:
        df_player = normalizar_seguimiento(df_player)
        df_player = df_player.sort_values("week_start", ascending=False)

        show_cols = [
            "week_start", "week_end",
            "partidos", "minutos",
            "goles_marcados", "goles_encajados",
            "amarillas", "rojas",
            "incidencias"
        ]
        st.dataframe(
            pretty_df(df_player, show_cols, hide_internal_ids=True),
            use_container_width=True,
            hide_index=True
        )

# =========================
# Página 3: Tabla acumulada
# =========================
elif page == pages[2]:
    st.subheader("📊 Tabla Acumulada")

    df_s2 = normalizar_seguimiento(df_s.copy())
    df_j2 = normalizar_jugadores(df_j.copy())

    if df_s2.empty:
        st.warning("Todavía no hay cargas semanales en 'seguimiento'.")
        st.stop()

    agg = df_s2.groupby("jugador_id", as_index=False).agg(
        partidos_total=("partidos", "sum"),
        minutos_total=("minutos", "sum"),
        goles_total=("goles_marcados", "sum"),
        encajados_total=("goles_encajados", "sum"),
        amarillas_total=("amarillas", "sum"),
        rojas_total=("rojas", "sum"),
        ultima_semana=("week_end", "max"),
    )

    base = df_j2.merge(agg, on="jugador_id", how="left")

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    with c1:
        estados = st.multiselect(
            "Estado",
            sorted(base["estado"].dropna().unique().tolist()),
            default=["Activo"] if "Activo" in base["estado"].unique() else None
        )
    with c2:
        puestos = st.multiselect("Puesto", sorted(base["puesto"].dropna().unique().tolist()))
    with c3:
        paises = st.multiselect("País", sorted(base["pais_prestamo"].dropna().unique().tolist()))
    with c4:
        solo_con_min = st.checkbox("Sólo con minutos > 0", value=True)

    df_view = base.copy()
    if estados:
        df_view = df_view[df_view["estado"].isin(estados)]
    if puestos:
        df_view = df_view[df_view["puesto"].isin(puestos)]
    if paises:
        df_view = df_view[df_view["pais_prestamo"].isin(paises)]
    if solo_con_min:
        df_view = df_view[pd.to_numeric(df_view["minutos_total"], errors="coerce").fillna(0) > 0]

    df_view["minutos_total"] = pd.to_numeric(df_view["minutos_total"], errors="coerce").fillna(0).astype(int)
    df_view = df_view.sort_values(["minutos_total", "partidos_total"], ascending=False)

    show_cols = [
        "nombre", "puesto",
        "pais_prestamo", "division_prestamo", "club_prestamo",
        "estado",
        "partidos_total", "minutos_total",
        "goles_total", "encajados_total",
        "amarillas_total", "rojas_total",
        "ultima_semana",
        "fecha_retorno", "fin_contrato_aaaj",
        "opcion_compra", "posibilidad_repesca",
    ]
    st.dataframe(pretty_df(df_view, show_cols, hide_internal_ids=True), use_container_width=True, hide_index=True)

# =========================
# Página 4: Vista individual
# =========================
elif page == pages[3]:
    st.subheader("👤 Vista Individual")

    if df_j.empty:
        st.warning("No hay jugadores.")
        st.stop()

    df_j2 = normalizar_jugadores(df_j.copy())
    df_j2["label"] = (
        df_j2["nombre"].astype(str)
        + " — "
        + df_j2["club_prestamo"].astype(str)
        + " ("
        + df_j2["puesto"].astype(str)
        + ")"
    )
    label_to_id = dict(zip(df_j2["label"], df_j2["jugador_id"]))

    jugador_label = st.selectbox("Jugador", df_j2["label"].tolist())
    jugador_id = label_to_id[jugador_label]
    j = df_j2[df_j2["jugador_id"].astype(str) == str(jugador_id)].iloc[0]

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    with c1:
        kpi_card("Puesto", str(j.get("puesto", "")))
    with c2:
        kpi_card("Club", str(j.get("club_prestamo", "")))
    with c3:
        kpi_card("País / División", f"{str(j.get('pais_prestamo',''))} — {str(j.get('division_prestamo',''))}")
    with c4:
        kpi_card("Fecha de retorno", str(j.get("fecha_retorno", "")))
    with c5:
        kpi_card("Fin de contrato en AAAJ", str(j.get("fin_contrato_aaaj", "")))
    with c6:
        kpi_card("¿Tiene opción de compra?", "Sí" if bool(j.get("opcion_compra", False)) else "No")
    with c7:
        kpi_card("¿Posibilidad de repesca?", "Sí" if bool(j.get("posibilidad_repesca", False)) else "No")

    if str(j.get("observaciones", "")).strip():
        st.info(f"**Observaciones:** {str(j.get('observaciones',''))}")

    df_player = df_s[df_s["jugador_id"].astype(str) == str(jugador_id)].copy()
    df_player = normalizar_seguimiento(df_player)

    if df_player.empty:
        st.warning("Este jugador aún no tiene cargas semanales.")
        st.stop()

    df_player = df_player.sort_values("week_start")

    show_cols = [
        "week_start", "week_end",
        "partidos", "minutos",
        "goles_marcados", "goles_encajados",
        "amarillas", "rojas",
        "incidencias"
    ]
    st.markdown("### Histórico semanal")
    st.dataframe(pretty_df(df_player, show_cols, hide_internal_ids=True), use_container_width=True, hide_index=True)

    st.markdown("### Tendencias")

    barh_with_labels_weekrange(
        df_player, "week_start", "week_end", "minutos",
        "Minutos por semana",
        title_size=11, axis_label_size=11, value_label_size=11
    )

    if is_gk(str(j.get("puesto", ""))):
        barh_with_labels_weekrange(
            df_player, "week_start", "week_end", "goles_encajados",
            "Goles encajados por semana",
            title_size=11, axis_label_size=11, value_label_size=11
        )
    else:
        barh_with_labels_weekrange(
            df_player, "week_start", "week_end", "goles_marcados",
            "Goles por semana",
            title_size=12, axis_label_size=11, value_label_size=11
        )

# =========================
# Página 5: Reportes
# =========================
elif page == pages[4]:
    st.subheader("📝 Reportes")

    if df_j.empty:
        st.warning("No hay jugadores.")
        st.stop()

    dfj2 = normalizar_jugadores(df_j.copy())
    dfj2["label"] = (
        dfj2["nombre"].astype(str)
        + " — "
        + dfj2["club_prestamo"].astype(str)
        + " ("
        + dfj2["puesto"].astype(str)
        + ")"
    )
    label_to_id = dict(zip(dfj2["label"], dfj2["jugador_id"]))

    jugador_label = st.selectbox("Jugador", dfj2["label"].tolist())
    jugador_id = label_to_id[jugador_label]

    st.markdown("### Reportes existentes")

    df_rep = df_r[df_r["jugador_id"].astype(str) == str(jugador_id)].copy()
    df_rep = normalizar_reportes(df_rep)

    if df_rep.empty:
        st.info("Este jugador aún no tiene reportes.")
    else:
        if "fecha_creacion" in df_rep.columns and df_rep["fecha_creacion"].notna().any():
            df_rep = df_rep.sort_values("fecha_creacion", ascending=False)
        else:
            df_rep = df_rep.sort_values("fecha_reporte", ascending=False)

        for _, r in df_rep.iterrows():
            titulo = str(r.get("titulo", "")).strip() or "Reporte"
            f_rep = r.get("fecha_reporte", pd.NaT)
            f_crea = r.get("fecha_creacion", pd.NaT)

            fecha_line = ""
            if isinstance(f_rep, date):
                fecha_line += f"Fecha: {f_rep.strftime('%d/%m/%Y')}"
            if isinstance(f_crea, datetime):
                fecha_line += (" | " if fecha_line else "") + f"Creado: {f_crea.strftime('%d/%m/%Y %H:%M')}"

            header = f"📄 {titulo}"
            if fecha_line:
                header += f" — {fecha_line}"

            with st.expander(header):
                st.write(str(r.get("contenido", "")))

    st.markdown("---")
    st.markdown("### ➕ Nuevo reporte")

    with st.form("form_nuevo_reporte", clear_on_submit=True):
        titulo = st.text_input("Título del reporte", placeholder="Ej: Informe mensual - Enero / Observación técnica / etc.")
        contenido = st.text_area(
            "Escribir reporte",
            height=240,
            placeholder="Observaciones técnicas, físicas, actitudinales, evolución, etc."
        )

        submit = st.form_submit_button("💾 Guardar reporte")
        if submit:
            if not titulo.strip():
                st.error("El título no puede estar vacío.")
            elif not contenido.strip():
                st.error("El reporte no puede estar vacío.")
            else:
                now = hoy_str()
                new_row = {
                    "reporte_id": str(uuid.uuid4()),
                    "jugador_id": str(jugador_id),
                    "titulo": titulo.strip(),
                    "fecha_reporte": date.today(),
                    "fecha_creacion": now,
                    "contenido": contenido.strip(),
                    "created_at": now,
                    "updated_at": now,
                }
                df_r_new = pd.concat([df_r, pd.DataFrame([new_row])], ignore_index=True)
                save_reportes(df_r_new)
                st.success("Reporte guardado ✅")

# =========================
# Página 6: Admin / export
# =========================
else:
    st.subheader("⚙️ Administración / Export")

    st.markdown("### Editar hoja de seguimiento")
    st.caption("Tabla editable para correcciones.")

    df_s_edit = df_s.copy()
    cols_front = [
        "week_start", "week_end", "jugador_id",
        "partidos", "minutos",
        "goles_marcados", "goles_encajados",
        "amarillas", "rojas", "incidencias",
        "registro_id",
    ]
    cols_front = [c for c in cols_front if c in df_s_edit.columns]
    cols_rest = [c for c in df_s_edit.columns if c not in cols_front]
    df_s_show = df_s_edit[cols_front + cols_rest].copy()

    edited_pretty = st.data_editor(
        pretty_df(df_s_show, cols_front + cols_rest, hide_internal_ids=False),
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            DISPLAY_LABELS.get("week_start", "Semana (inicio)"): st.column_config.DateColumn("Semana (inicio)"),
            DISPLAY_LABELS.get("week_end", "Semana (fin)"): st.column_config.DateColumn("Semana (fin)"),
        },
        disabled=[DISPLAY_LABELS.get("registro_id", "ID registro")],
    )

    inverse_labels = {v: k for k, v in DISPLAY_LABELS.items()}
    edited = edited_pretty.rename(columns={c: inverse_labels.get(c, c) for c in edited_pretty.columns})

    if st.button("💾 Guardar cambios (Seguimiento)", type="primary"):
        df_new = edited.copy()
        df_new["updated_at"] = hoy_str()
        if "created_at" in df_new.columns:
            df_new["created_at"] = df_new["created_at"].replace("", np.nan).fillna(hoy_str())
        for c in REQUIRED_SEGUIMIENTO_COLS:
            if c not in df_new.columns:
                df_new[c] = ""
        df_new = df_new[REQUIRED_SEGUIMIENTO_COLS]
        save_seguimiento(df_new)
        st.success("Seguimiento guardado ✅")

    st.markdown("---")
    st.markdown("### Export (Excel)")

    excel_bytes = make_excel_bytes(df_j, df_s, df_r)
    st.download_button(
        "⬇️ Descargar Excel (Jugadores + Seguimiento + Reportes)",
        data=excel_bytes,
        file_name="prestamos_aaaj.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

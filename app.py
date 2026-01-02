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

# =========================
# Configuración Streamlit
# =========================
st.set_page_config(page_title="Seguimiento de Préstamos", layout="wide")
st.title("📌 Seguimiento de Préstamos (Google Sheets)")

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

REQUIRED_JUGADORES_COLS = [
    "jugador_id",
    "nombre",
    "puesto",
    "fecha_nacimiento",
    "pais_prestamo",
    "division_prestamo",
    "club_prestamo",
    "opcion_compra",
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


@st.cache_resource
def get_gspread_client():
    creds_info = dict(st.secrets["google_service_account"])
    credentials = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(credentials)


@st.cache_resource
def get_workbook():
    if not SHEET_NAME:
        st.error('Falta configurar GSPREAD_SHEET_NAME en secrets')
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
    return ws_j, ws_s


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
    ws_j, ws_s = init_sheets()
    df_j = _ws_to_df(ws_j)
    df_s = _ws_to_df(ws_s)
    return df_j, df_s


def save_jugadores(df_j):
    ws_j, _ = init_sheets()
    _df_to_ws_overwrite(ws_j, df_j, REQUIRED_JUGADORES_COLS)
    st.cache_data.clear()


def save_seguimiento(df_s):
    _, ws_s = init_sheets()
    _df_to_ws_overwrite(ws_s, df_s, REQUIRED_SEGUIMIENTO_COLS)
    st.cache_data.clear()


def normalizar_jugadores(df_j):
    if df_j.empty:
        df_j = pd.DataFrame(columns=REQUIRED_JUGADORES_COLS)

    for col in ["fecha_nacimiento", "fecha_retorno", "fin_contrato_aaaj"]:
        if col in df_j.columns:
            df_j[col] = df_j[col].apply(_parse_date_safe)

    if "opcion_compra" in df_j.columns:
        df_j["opcion_compra"] = (
            df_j["opcion_compra"].astype(str).str.strip().str.lower().map(
                {"true": True, "false": False, "1": True, "0": False, "si": True, "sí": True, "no": False}
            ).fillna(False)
        )

    return df_j


def normalizar_seguimiento(df_s):
    if df_s.empty:
        df_s = pd.DataFrame(columns=REQUIRED_SEGUIMIENTO_COLS)

    if "week_end" in df_s.columns:
        df_s["week_end"] = df_s["week_end"].apply(_parse_date_safe)

    num_cols = ["partidos", "minutos", "goles_marcados", "goles_encajados", "amarillas", "rojas"]
    for c in num_cols:
        if c in df_s.columns:
            df_s[c] = pd.to_numeric(df_s[c], errors="coerce").fillna(0).astype(int)

    return df_s


def hoy_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_week_end(d=None):
    if d is None:
        d = date.today()
    if isinstance(d, datetime):
        d = d.date()
    days_to_sun = 6 - d.weekday()
    return d + timedelta(days=days_to_sun)


def is_gk(puesto: str) -> bool:
    if puesto is None:
        return False
    p = str(puesto).strip().lower()
    return "arquero" in p or p in ["gk", "goalkeeper", "portero"]


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
    df_new = df_new[REQUIRED_JUGADORES_COLS]
    return df_new


# =========================
# UI: Navegación
# =========================
pages = [
    "➕ Alta / Edición de Jugadores",
    "🗓️ Carga Semanal",
    "📊 Tabla Acumulada",
    "👤 Vista Individual",
    "⚙️ Administración / Export",
]
page = st.sidebar.radio("Navegación", pages)

df_j_raw, df_s_raw = load_data()
df_j = normalizar_jugadores(df_j_raw)
df_s = normalizar_seguimiento(df_s_raw)

for c in REQUIRED_JUGADORES_COLS:
    if c not in df_j.columns:
        df_j[c] = ""
for c in REQUIRED_SEGUIMIENTO_COLS:
    if c not in df_s.columns:
        df_s[c] = ""


# =========================
# Página 1: Jugadores (FORM ARRIBA + TABLA ABAJO) + sin "doble guardado"
# =========================
if page == pages[0]:
    st.subheader("➕ Alta / Edición de Jugadores")

    tab_crear, tab_editar = st.tabs(["Crear jugador", "Editar jugador"])

    # ---------- CREAR ----------
    with tab_crear:
        st.markdown("### Crear jugador (se guarda al enviar)")
        with st.form("form_crear_jugador", clear_on_submit=True):
            nombre = st.text_input("Nombre", placeholder="Ej: Juan Pérez")
            puesto = st.selectbox("Puesto", PUESTOS)
            fecha_nac = st.date_input("Fecha de nacimiento", value=date(2000, 1, 1))

            pais = st.text_input("País (préstamo)", placeholder="Ej: Argentina / Uruguay / Chile")
            division = st.selectbox("División", DIVISIONES, index=0)
            club_prestamo = st.text_input("Club (préstamo)", placeholder="Ej: Club X")

            opcion_compra = st.checkbox("Tiene opción de compra")
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
                        "fecha_retorno": fecha_retorno,
                        "fin_contrato_aaaj": fin_contrato,
                        "estado": estado,
                        "observaciones": obs.strip(),
                    }
                    df_new = upsert_jugador(df_j, jugador_id, payload)
                    save_jugadores(df_new)
                    st.success("Jugador guardado ✅")

    # ---------- EDITAR ----------
    with tab_editar:
        st.markdown("### Editar jugador (se guarda al enviar)")

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

            # valores por defecto seguros
            def _date_or_default(x, default):
                return x if isinstance(x, date) else default

            with st.form("form_editar_jugador", clear_on_submit=False):
                nombre = st.text_input("Nombre", value=str(j.get("nombre", "")))
                puesto = st.selectbox("Puesto", PUESTOS, index=max(0, PUESTOS.index(str(j.get("puesto", PUESTOS[0]))) if str(j.get("puesto", PUESTOS[0])) in PUESTOS else 0))
                fecha_nac = st.date_input("Fecha de nacimiento", value=_date_or_default(j.get("fecha_nacimiento", pd.NaT), date(2000, 1, 1)))

                pais = st.text_input("País (préstamo)", value=str(j.get("pais_prestamo", "")))
                div_val = str(j.get("division_prestamo", DIVISIONES[0]))
                division = st.selectbox("División", DIVISIONES, index=DIVISIONES.index(div_val) if div_val in DIVISIONES else 0)
                club_prestamo = st.text_input("Club (préstamo)", value=str(j.get("club_prestamo", "")))

                opcion_compra = st.checkbox("Tiene opción de compra", value=bool(j.get("opcion_compra", False)))
                fecha_retorno = st.date_input("Fecha de retorno", value=_date_or_default(j.get("fecha_retorno", pd.NaT), date.today() + relativedelta(months=6)))
                fin_contrato = st.date_input("Fin de contrato con AAAJ", value=_date_or_default(j.get("fin_contrato_aaaj", pd.NaT), date.today() + relativedelta(years=2)))
                estado = st.selectbox("Estado", ["Activo", "Finalizado", "Rescindido"], index=["Activo", "Finalizado", "Rescindido"].index(str(j.get("estado", "Activo"))) if str(j.get("estado", "Activo")) in ["Activo", "Finalizado", "Rescindido"] else 0)
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
                            "fecha_retorno": fecha_retorno,
                            "fin_contrato_aaaj": fin_contrato,
                            "estado": estado,
                            "observaciones": obs.strip(),
                        }
                        df_new = upsert_jugador(df_j, jugador_id, payload)
                        save_jugadores(df_new)
                        st.success("Cambios guardados ✅")

    st.markdown("---")
    st.markdown("### Tabla de jugadores (visualización)")
    st.caption("Esta tabla es solo para ver/filtrar. Para editar, usá el tab de **Editar jugador** (arriba).")

    df_view = normalizar_jugadores(df_j.copy())

    if df_view.empty:
        st.info("No hay jugadores cargados todavía.")
    else:
        # tabla más amigable
        show_cols = [
            "nombre", "puesto",
            "pais_prestamo", "division_prestamo", "club_prestamo",
            "estado", "opcion_compra",
            "fecha_retorno", "fin_contrato_aaaj",
            "observaciones", "jugador_id"
        ]
        for c in show_cols:
            if c not in df_view.columns:
                df_view[c] = ""

        st.dataframe(
            df_view[show_cols].sort_values(["estado", "nombre"], ascending=[True, True]),
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

    colA, colB = st.columns([1, 1], gap="large")

    with colA:
        jugador_label = st.selectbox("Jugador", activos["label"].tolist())
        jugador_id = label_to_id[jugador_label]
        jugador_row = activos.loc[activos["jugador_id"] == jugador_id].iloc[0]
        puesto = str(jugador_row["puesto"])

        st.markdown("### Cargar semana")
        with st.form("form_carga_semanal", clear_on_submit=True):
            week_end = st.date_input("Semana (fin de semana)", value=get_week_end())
            partidos = st.number_input("Partidos", min_value=0, max_value=10, value=0, step=1)
            minutos = st.number_input("Minutos", min_value=0, max_value=900, value=0, step=1)

            if is_gk(puesto):
                goles_encajados = st.number_input("Goles encajados", min_value=0, max_value=50, value=0, step=1)
                goles_marcados = 0
            else:
                goles_marcados = st.number_input("Goles marcados", min_value=0, max_value=50, value=0, step=1)
                goles_encajados = 0

            amarillas = st.number_input("Amarillas", min_value=0, max_value=10, value=0, step=1)
            rojas = st.number_input("Rojas", min_value=0, max_value=10, value=0, step=1)
            incidencias = st.text_area("Incidencias / notas", placeholder="Lesión, debut, asistencia, expulsión, etc.")

            submit = st.form_submit_button("Guardar semana")
            if submit:
                exists = (
                    (df_s["jugador_id"].astype(str) == str(jugador_id)) &
                    (df_s["week_end"].apply(_parse_date_safe) == week_end)
                )
                if exists.any():
                    st.error("Ya existe un registro para ese jugador en esa semana. Corregilo desde Admin/Export.")
                else:
                    now = hoy_str()
                    new_row = {
                        "registro_id": str(uuid.uuid4()),
                        "jugador_id": str(jugador_id),
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

    with colB:
        st.markdown("### Últimos registros del jugador")
        df_player = df_s[df_s["jugador_id"].astype(str) == str(jugador_id)].copy()
        if df_player.empty:
            st.info("Aún no hay cargas para este jugador.")
        else:
            df_player = normalizar_seguimiento(df_player)
            df_player = df_player.sort_values("week_end", ascending=False)
            show_cols = ["week_end", "partidos", "minutos", "goles_marcados", "goles_encajados", "amarillas", "rojas", "incidencias"]
            st.dataframe(df_player[show_cols], use_container_width=True, hide_index=True)

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
        estados = st.multiselect("Estado", sorted(base["estado"].dropna().unique().tolist()), default=["Activo"] if "Activo" in base["estado"].unique() else None)
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
        "opcion_compra",
    ]

    st.dataframe(df_view[show_cols], use_container_width=True, hide_index=True)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Jugadores en vista", int(df_view.shape[0]))
    m2.metric("Minutos totales", int(df_view["minutos_total"].sum()))
    m3.metric("Partidos totales", int(pd.to_numeric(df_view["partidos_total"], errors="coerce").fillna(0).sum()))
    m4.metric("Rojas totales", int(pd.to_numeric(df_view["rojas_total"], errors="coerce").fillna(0).sum()))

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

    a, b, c, d = st.columns(4)
    a.metric("Puesto", str(j["puesto"]))
    b.metric("Club (préstamo)", str(j["club_prestamo"]))
    c.metric("País / División", f"{str(j.get('pais_prestamo',''))} — {str(j.get('division_prestamo',''))}")
    d.metric("Retorno", str(j["fecha_retorno"]))

    st.write("**Fin contrato AAAJ:**", str(j["fin_contrato_aaaj"]))
    st.write("**Opción de compra:**", "Sí" if bool(j["opcion_compra"]) else "No")
    if str(j.get("observaciones", "")).strip():
        st.info(f"**Observaciones:** {str(j['observaciones'])}")

    df_player = df_s[df_s["jugador_id"].astype(str) == str(jugador_id)].copy()
    df_player = normalizar_seguimiento(df_player)

    if df_player.empty:
        st.warning("Este jugador aún no tiene cargas semanales.")
        st.stop()

    df_player = df_player.sort_values("week_end")
    show_cols = ["week_end", "partidos", "minutos", "goles_marcados", "goles_encajados", "amarillas", "rojas", "incidencias"]
    st.markdown("### Histórico semanal")
    st.dataframe(df_player[show_cols], use_container_width=True, hide_index=True)

    st.markdown("### Tendencias")
    g1, g2 = st.columns(2)
    with g1:
        st.line_chart(df_player.set_index("week_end")["minutos"])
    with g2:
        if is_gk(str(j["puesto"])):
            st.line_chart(df_player.set_index("week_end")["goles_encajados"])
        else:
            st.line_chart(df_player.set_index("week_end")["goles_marcados"])

# =========================
# Página 5: Admin / export
# =========================
else:
    st.subheader("⚙️ Administración / Export")

    st.markdown("### Editar hoja de seguimiento (tabla)")
    st.caption("Esta es la única tabla editable. Si preferís, después lo pasamos a edición por formulario también.")

    df_s_edit = df_s.copy()
    cols_front = [
        "week_end", "jugador_id", "partidos", "minutos",
        "goles_marcados", "goles_encajados",
        "amarillas", "rojas", "incidencias",
        "registro_id",
    ]
    cols_front = [c for c in cols_front if c in df_s_edit.columns]
    cols_rest = [c for c in df_s_edit.columns if c not in cols_front]
    df_s_show = df_s_edit[cols_front + cols_rest].copy()

    edited = st.data_editor(
        df_s_show,
        use_container_width=True,
        num_rows="dynamic",
        column_config={"week_end": st.column_config.DateColumn("Semana (week_end)")},
        disabled=["registro_id"],
    )

    c1, c2 = st.columns([1, 1])
    with c1:
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

    with c2:
        st.markdown("### Export")
        st.download_button(
            "⬇️ Descargar jugadores (CSV)",
            data=df_j.to_csv(index=False).encode("utf-8"),
            file_name="jugadores.csv",
            mime="text/csv",
        )
        st.download_button(
            "⬇️ Descargar seguimiento (CSV)",
            data=df_s.to_csv(index=False).encode("utf-8"),
            file_name="seguimiento.csv",
            mime="text/csv",
        )

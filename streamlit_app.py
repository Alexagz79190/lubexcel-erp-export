import streamlit as st
import requests
import re
import csv
import ftplib
import json
from datetime import datetime
from io import StringIO, BytesIO
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="LUBEXCEL – Export Livraisons", page_icon="🚚", layout="wide")

# ── Chargement secrets Streamlit ──────────────────────────────────
zd_subdomain = st.secrets["zendesk"]["subdomain"]
zd_email     = st.secrets["zendesk"]["email"]
zd_token     = st.secrets["zendesk"]["api_token"]
ftp_host     = st.secrets["ftp"]["host"]
ftp_user     = st.secrets["ftp"]["user"]
ftp_pass     = st.secrets["ftp"]["password"]
ftp_port     = int(st.secrets["ftp"]["port"])
bq_project   = st.secrets["bigquery"]["project_id"]
bq_dataset   = st.secrets["bigquery"]["dataset_id"]
bq_sa        = st.secrets["bigquery"]["service_account_json"]

# ── Client BigQuery ───────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    info        = json.loads(bq_sa) if isinstance(bq_sa, str) else dict(bq_sa)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=credentials, project=bq_project)

def get_prices(refs: list) -> dict:
    if not refs:
        return {}
    refs_sql = ", ".join(f"'{r}'" for r in refs)
    query = f"""
        SELECT CAST(ref_fournisseur AS STRING) AS ref_fournisseur, prix_achat_ht
        FROM `{bq_project}.{bq_dataset}.web_agrizone_produit_description`
        WHERE CAST(ref_fournisseur AS STRING) IN ({refs_sql})
    """
    try:
        df = get_bq_client().query(query).result().to_dataframe()
        return dict(zip(df["ref_fournisseur"].astype(str), df["prix_achat_ht"]))
    except Exception as e:
        st.warning(f"⚠️ BigQuery : {e}")
        return {}

# ── Zendesk ───────────────────────────────────────────────────────
RE_PRODUCT = re.compile(r"(\d+)\s*x\s+.+?\((\d+)\)", re.IGNORECASE)
RE_ORDER   = re.compile(r"\bAG\d+(?:/[A-Z0-9]+)?", re.IGNORECASE)

def fetch_tickets():
    auth  = (f"{zd_email}/token", zd_token)
    query = 'type:ticket status:new subject:"[ LUBEXCEL ] Votre commande AG"'
    url   = f"https://{zd_subdomain}.zendesk.com/api/v2/search.json"
    tickets = []
    while url:
        r = requests.get(url, auth=auth, params={"query": query, "per_page": 100}, timeout=30)
        r.raise_for_status()
        data = r.json()
        tickets.extend([t for t in data.get("results", [])
                        if t.get("subject", "").startswith("[ LUBEXCEL ] Votre commande AG")])
        url = data.get("next_page")
    return tickets

def parse_ticket(ticket):
    body       = ticket.get("description", "")
    m_order    = RE_ORDER.search(ticket.get("subject", ""))
    order_full = m_order.group(0).upper() if m_order else ""
    order_base = order_full.split("/")[0]
    rows = []
    for qty, ref in RE_PRODUCT.findall(body):
        rows.append({"ticket_id": ticket["id"], "order_full": order_full,
                     "order_base": order_base, "ref": ref, "qty": int(qty)})
    return rows

def resolve_ticket(ticket_id):
    requests.put(
        f"https://{zd_subdomain}.zendesk.com/api/v2/tickets/{ticket_id}.json",
        auth=(f"{zd_email}/token", zd_token),
        json={"ticket": {"status": "solved"}}, timeout=30
    )

# ── FTP ───────────────────────────────────────────────────────────
def send_ftp(content, filename):
    buf = BytesIO(content.encode("utf-8"))
    with ftplib.FTP() as ftp:
        ftp.connect(ftp_host, ftp_port, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        ftp.cwd("/ftpfou/topaz")
        ftp.storbinary(f"STOR {filename}", buf)

# ── Interface ─────────────────────────────────────────────────────
st.title("🚚 LUBEXCEL — Export Livraisons vers ERP")
st.markdown("Récupère les commandes Zendesk, enrichit les prix BigQuery et envoie le fichier à l'ERP.")
st.divider()

resolve_cb = st.checkbox("✅ Résoudre les tickets Zendesk après export", value=True)

if st.button("🚀 Lancer l'export", type="primary", use_container_width=True):
    logs   = []
    status = st.empty()
    bar    = st.progress(0, text="Démarrage…")

    def log(msg, pct):
        logs.append(msg)
        status.info("\n".join(logs))
        bar.progress(pct, text=msg)

    try:
        log("🔍 Récupération des tickets Zendesk…", 10)
        tickets = fetch_tickets()
        log(f"✅ {len(tickets)} ticket(s) trouvé(s)", 25)

        all_rows = []
        for t in tickets:
            all_rows.extend(parse_ticket(t))
        log(f"✅ {len(all_rows)} ligne(s) extraite(s)", 40)

        refs   = list({r["ref"] for r in all_rows})
        prices = get_prices(refs)
        log(f"✅ {len(prices)}/{len(refs)} prix récupérés depuis BigQuery", 60)

        output = StringIO()
        writer = csv.writer(output, delimiter=";")
        for r in all_rows:
            writer.writerow([
                r["order_full"], "", r["order_base"], r["order_full"],
                "", "", r["ref"], r["qty"], prices.get(r["ref"], "")
            ])
        csv_content = output.getvalue()
        filename = f"OU_LIV_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        log(f"✅ CSV généré : {filename}", 75)

        try:
            send_ftp(csv_content, filename)
            log("✅ Fichier envoyé sur FTP /ftpfou/topaz", 90)
        except Exception as e:
            log(f"❌ Erreur FTP : {e}", 90)

        if resolve_cb and all_rows:
            ids = list({r["ticket_id"] for r in all_rows})
            for tid in ids:
                resolve_ticket(tid)
            log(f"✅ {len(ids)} ticket(s) résolu(s)", 100)

        bar.empty()
        status.success("🎉 Export terminé avec succès !")

        st.divider()
        st.subheader("📊 Résultats")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🎫 Tickets",  len(tickets))
        c2.metric("📦 Lignes",   len(all_rows))
        c3.metric("💰 Prix",     len(prices))
        c4.metric("✅ Résolus",  len({r["ticket_id"] for r in all_rows}) if resolve_cb else 0)

        st.subheader("📋 Aperçu")
        st.dataframe(pd.DataFrame(all_rows), use_container_width=True, hide_index=True)
        st.download_button("⬇️ Télécharger le CSV", csv_content, filename, "text/csv",
                           use_container_width=True)

    except Exception as e:
        st.error(f"❌ Erreur : {e}")
        bar.empty()

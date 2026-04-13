import os
import shutil
import socket
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "InsuDesk"
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "insudesk.db"
BACKUP_DIR = BASE_DIR / "backup"

ALLOWED_EMAILS = [
    "matsumoto@lifepartners.me",  # ← 必ず自分のGmailへ変更
]

CORPORATE_INSURANCE_OPTIONS = [
    "自動車AAP・AAI",
    "業務災害総合保険（ハイパー任意労災）",
    "マネジメントリスクプロテクション保険",
    "労働災害総合保険",
    "企業財産保険（ニュープロパティーガード）",
    "企業財産保険（プロパティーガード）",
    "企業財産包括保険",
    "国内物流総合運送保険",
    "事業賠償・費用総合保険（ALL STARs）",
    "個人情報漏洩保険",
]

PERSONAL_INSURANCE_OPTIONS = [
    "自動車AAP・AAI",
    "ホームプロテクト総合保険",
    "リビングパートナー保険",
    "地震保険",
    "傷害総合保険",
    "部位・症状別保険金支払特約付傷害総合保険",
    "医療保険（実費補償型）",
    "医療保険（引受基準緩和型）",
    "海外旅行保険セットプラン",
    "海外旅行保険ファミリープラン",
    "海外留学保険",
    "海外出張・駐在保険",
    "海外旅行保険特約",
]

LIFE_INSURANCE_OPTIONS = [
    "第一分野",
    "第三分野",
]

ALL_INSURANCE_OPTIONS = list(
    dict.fromkeys(
        CORPORATE_INSURANCE_OPTIONS
        + PERSONAL_INSURANCE_OPTIONS
        + LIFE_INSURANCE_OPTIONS
    )
)

INDUSTRY_OPTIONS = ["製造", "建設", "運送", "医療", "その他"]
NANKAI_PRIORITY_OPTIONS = ["高", "中", "低"]
ACTIVITY_TYPE_OPTIONS = ["訪問", "TEL", "メール", "提案", "更改", "事故対応", "その他"]
RESULT_OPTIONS = ["見積依頼", "再訪問", "保留", "契約", "失注", "対応完了"]
TEMPERATURE_OPTIONS = ["高", "中", "低"]
DM_TYPE_OPTIONS = ["更改案内", "BCP案内", "南海トラフ対策", "生命保険案内", "事故防止情報", "ニュースレター", "その他"]
DM_REACTION_OPTIONS = ["反応なし", "見積依頼", "再訪問", "契約", "保留"]
OPPORTUNITY_STATUS_OPTIONS = ["見込", "見積中", "提案済", "検討中", "成約", "失注"]
INSURANCE_PROGRESS_OPTIONS = ["加入中", "提案中", "未提案"]
KEISHO_OPTIONS = ["様", "御中"]

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"], [class*="st-"], .main, .block-container {
        font-family: "Meiryo", "メイリオ", sans-serif !important;
    }
    .small-card {
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 12px;
        margin-bottom: 10px;
        background: #fafafa;
    }
    .calendar-day {
        min-height: 150px;
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 8px;
        background: white;
    }
    .calendar-muted {
        color: #999;
    }
    .event-box {
        font-size: 12px;
        border-radius: 6px;
        padding: 4px 6px;
        margin-top: 4px;
        background: #f2f4f8;
        border-left: 4px solid #4e79a7;
    }
    .event-visit { border-left-color: #4e79a7; }
    .event-todo { border-left-color: #59a14f; }
    .event-dm { border-left-color: #f28e2b; }
    .event-renewal { border-left-color: #e15759; }
    .event-alert { border-left-color: #af7aa1; background: #faf1fb; }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================
# Googleログイン
# =========================================
def safe_user_get(key: str, default=""):
    try:
        return st.user.get(key, default)
    except Exception:
        return default


def auth_configured() -> bool:
    try:
        auth = st.secrets["auth"]
        required = [
            "redirect_uri",
            "cookie_secret",
            "client_id",
            "client_secret",
            "server_metadata_url",
        ]
        return all(str(auth.get(k, "")).strip() for k in required)
    except Exception:
        return False


if not auth_configured():
    st.title(APP_TITLE)
    st.error("ログイン環境を確認してください。")
    st.code(
        r"""
1) D:\kpi_app\.streamlit\secrets.toml がある
2) [auth] に redirect_uri / cookie_secret / client_id / client_secret / server_metadata_url が入っている
3) python -m pip install --upgrade streamlit authlib
"""
    )
    st.stop()

is_logged_in = bool(getattr(st.user, "is_logged_in", False))

if not is_logged_in:
    st.title(APP_TITLE)
    st.write("利用するには Google でログインしてください。")
    if st.button("Googleでログイン"):
        st.login()
    st.stop()

user_email = safe_user_get("email", "")
user_name = safe_user_get("name", "User")

if user_email not in ALLOWED_EMAILS:
    st.error("このアカウントは利用できません。")
    st.write(f"ログイン中: {user_email}")
    if st.button("ログアウト"):
        st.logout()
    st.stop()

# =========================================
# DB初期化
# =========================================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    BACKUP_DIR.mkdir(exist_ok=True)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS kpi_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        staff TEXT,
        new_cases INTEGER DEFAULT 0,
        teleapo INTEGER DEFAULT 0,
        visits INTEGER DEFAULT 0,
        contracts INTEGER DEFAULT 0,
        sonpo_premium REAL DEFAULT 0,
        sonpo_commission REAL DEFAULT 0,
        seiho_new_s REAL DEFAULT 0,
        seiho_commission REAL DEFAULT 0,
        inforce_premium REAL DEFAULT 0,
        memo TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS targets (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        new_cases_target INTEGER DEFAULT 5,
        teleapo_target INTEGER DEFAULT 100,
        visits_target INTEGER DEFAULT 30,
        contracts_target INTEGER DEFAULT 6,
        sonpo_premium_target REAL DEFAULT 3000000,
        sonpo_commission_target REAL DEFAULT 50000,
        seiho_new_s_target REAL DEFAULT 1000000,
        seiho_commission_target REAL DEFAULT 800000,
        inforce_premium_target REAL DEFAULT 30000000
    )
    """)

    cur.execute("""
    INSERT OR IGNORE INTO targets (
        id, new_cases_target, teleapo_target, visits_target, contracts_target,
        sonpo_premium_target, sonpo_commission_target, seiho_new_s_target,
        seiho_commission_target, inforce_premium_target
    ) VALUES (1, 5, 100, 30, 6, 3000000, 50000, 1000000, 800000, 30000000)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT,
        company_name TEXT,
        department_name TEXT,
        attn_name TEXT,
        keisho TEXT,
        contact_name TEXT,
        rep_phone TEXT,
        contact_phone TEXT,
        phone TEXT,
        email TEXT,
        postal_code TEXT,
        address1 TEXT,
        address2 TEXT,
        staff TEXT,
        status TEXT,
        customer_rank TEXT,
        insurance_types TEXT,
        industry TEXT,
        annual_sales REAL DEFAULT 0,
        employee_count INTEGER DEFAULT 0,
        nankai_priority TEXT,
        bcp_exists TEXT,
        continuity_plan_applied TEXT,
        sonpo_annual_premium REAL DEFAULT 0,
        seiho_annual_premium REAL DEFAULT 0,
        renewal_month INTEGER DEFAULT 0,
        last_contact_date TEXT,
        next_action TEXT,
        next_action_date TEXT,
        memo TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS activity_log (
        activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        customer_id INTEGER,
        customer_name TEXT,
        activity_type TEXT,
        staff TEXT,
        insurance_type TEXT,
        memo TEXT,
        next_action TEXT,
        next_date TEXT,
        result TEXT,
        temperature TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS opportunities (
        opportunity_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        customer_name TEXT,
        insurance_type TEXT,
        status TEXT,
        estimated_premium REAL DEFAULT 0,
        estimated_commission REAL DEFAULT 0,
        renewal_month INTEGER DEFAULT 0,
        probability INTEGER DEFAULT 0,
        memo TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS insurance_status (
        status_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        customer_name TEXT,
        insurance_type TEXT,
        progress TEXT,
        memo TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dm_history (
        dm_id INTEGER PRIMARY KEY AUTOINCREMENT,
        send_date TEXT,
        customer_id INTEGER,
        customer_name TEXT,
        dm_type TEXT,
        title TEXT,
        memo TEXT,
        staff TEXT,
        followup_due_date TEXT,
        followup_done TEXT,
        reaction TEXT
    )
    """)

    conn.commit()
    conn.close()


init_db()

# =========================================
# 共通関数
# =========================================
def safe_int(value):
    try:
        if pd.isna(value):
            return 0
        return int(float(value))
    except Exception:
        return 0


def parse_date_str(value):
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()


def format_date_str(value):
    d = parse_date_str(value)
    return d.strftime("%Y-%m-%d") if d else ""


def days_since(value):
    d = parse_date_str(value)
    if not d:
        return None
    return (date.today() - d).days


def calc_rate(numerator, denominator):
    if denominator in [0, None]:
        return 0.0
    return round((numerator / denominator) * 100, 1)


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "取得できません"


@st.cache_data(ttl=3)
def load_table(query, params=()):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def clear_all_cache():
    load_table.clear()


def get_kpi_df():
    return load_table("SELECT * FROM kpi_data ORDER BY id DESC")


def get_targets_df():
    return load_table("SELECT * FROM targets WHERE id=1")


def get_customers_df():
    return load_table("SELECT * FROM customers ORDER BY customer_id DESC")


def get_activity_df():
    return load_table("SELECT * FROM activity_log ORDER BY activity_id DESC")


def get_opportunities_df():
    return load_table("SELECT * FROM opportunities ORDER BY opportunity_id DESC")


def get_insurance_status_df():
    return load_table("SELECT * FROM insurance_status ORDER BY status_id DESC")


def get_dm_history_df():
    return load_table("SELECT * FROM dm_history ORDER BY dm_id DESC")

# =========================================
# バックアップ機能
# =========================================
def create_backup():
    BACKUP_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"insudesk_backup_{ts}.db"
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def list_backups():
    BACKUP_DIR.mkdir(exist_ok=True)
    files = sorted(BACKUP_DIR.glob("*.db"), reverse=True)
    rows = []
    for f in files:
        stat = f.stat()
        rows.append({
            "filename": f.name,
            "path": str(f),
            "size_kb": round(stat.st_size / 1024, 1),
            "updated": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)

# =========================================
# 保存処理
# =========================================
def insert_kpi(row):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO kpi_data (
            date, staff, new_cases, teleapo, visits, contracts,
            sonpo_premium, sonpo_commission, seiho_new_s,
            seiho_commission, inforce_premium, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)
    conn.commit()
    conn.close()
    clear_all_cache()


def update_targets(values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE targets SET
            new_cases_target=?,
            teleapo_target=?,
            visits_target=?,
            contracts_target=?,
            sonpo_premium_target=?,
            sonpo_commission_target=?,
            seiho_new_s_target=?,
            seiho_commission_target=?,
            inforce_premium_target=?
        WHERE id=1
    """, values)
    conn.commit()
    conn.close()
    clear_all_cache()


def insert_customer(values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO customers (
            category, company_name, department_name, attn_name, keisho,
            contact_name, rep_phone, contact_phone, phone, email,
            postal_code, address1, address2, staff, status, customer_rank,
            insurance_types, industry, annual_sales, employee_count,
            nankai_priority, bcp_exists, continuity_plan_applied,
            sonpo_annual_premium, seiho_annual_premium, renewal_month,
            last_contact_date, next_action, next_action_date, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()
    clear_all_cache()


def update_customer(customer_id, values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE customers SET
            category=?, company_name=?, department_name=?, attn_name=?, keisho=?,
            contact_name=?, rep_phone=?, contact_phone=?, phone=?, email=?,
            postal_code=?, address1=?, address2=?, staff=?, status=?, customer_rank=?,
            insurance_types=?, industry=?, annual_sales=?, employee_count=?,
            nankai_priority=?, bcp_exists=?, continuity_plan_applied=?,
            sonpo_annual_premium=?, seiho_annual_premium=?, renewal_month=?,
            last_contact_date=?, next_action=?, next_action_date=?, memo=?
        WHERE customer_id=?
    """, values + (customer_id,))
    conn.commit()
    conn.close()
    clear_all_cache()


def delete_customer(customer_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM customers WHERE customer_id=?", (customer_id,))
    conn.commit()
    conn.close()
    clear_all_cache()


def insert_activity(values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO activity_log (
            date, customer_id, customer_name, activity_type, staff,
            insurance_type, memo, next_action, next_date, result, temperature
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()
    clear_all_cache()


def insert_opportunity(values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO opportunities (
            customer_id, customer_name, insurance_type, status,
            estimated_premium, estimated_commission, renewal_month,
            probability, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()
    clear_all_cache()


def insert_insurance_status(values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO insurance_status (
            customer_id, customer_name, insurance_type, progress, memo
        ) VALUES (?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()
    clear_all_cache()


def insert_dm(values):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dm_history (
            send_date, customer_id, customer_name, dm_type, title,
            memo, staff, followup_due_date, followup_done, reaction
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()
    clear_all_cache()

# =========================================
# 表示用
# =========================================
def to_monthly(df):
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"])
    if work.empty:
        return pd.DataFrame()

    work["month"] = work["date"].dt.to_period("M").astype(str)
    monthly = work.groupby("month", as_index=False).agg({
        "new_cases": "sum",
        "teleapo": "sum",
        "visits": "sum",
        "contracts": "sum",
        "sonpo_premium": "sum",
        "sonpo_commission": "sum",
        "seiho_new_s": "sum",
        "seiho_commission": "sum",
        "inforce_premium": "max",
    })
    monthly["アポ→訪問率"] = monthly.apply(lambda x: calc_rate(x["visits"], x["teleapo"]), axis=1)
    monthly["訪問→成約率"] = monthly.apply(lambda x: calc_rate(x["contracts"], x["visits"]), axis=1)
    return monthly


def make_forecast(current_inforce, monthly_new_cases, avg_premium_per_case, avg_commission_per_case, annual_retention_rate):
    rows = []
    inforce = safe_int(current_inforce)
    monthly_new_cases = safe_int(monthly_new_cases)
    avg_premium_per_case = safe_int(avg_premium_per_case)
    avg_commission_per_case = safe_int(avg_commission_per_case)
    retention = annual_retention_rate / 100

    annual_new_premium = monthly_new_cases * avg_premium_per_case * 12
    annual_new_commission = monthly_new_cases * avg_commission_per_case * 12

    for year in range(1, 6):
        retained_inforce = int(inforce * retention)
        end_inforce = retained_inforce + annual_new_premium
        rows.append({
            "年": f"{year}年目",
            "期首保有収保": inforce,
            "継続後保有収保": retained_inforce,
            "年間新規保険料": annual_new_premium,
            "期末保有収保": end_inforce,
            "年間新規件数": monthly_new_cases * 12,
            "年間損保手数料見込": annual_new_commission,
        })
        inforce = end_inforce

    return pd.DataFrame(rows)


def build_todo_df(customers_df, activity_df, dm_df):
    rows = []

    if not customers_df.empty:
        for _, row in customers_df.iterrows():
            if str(row.get("next_action", "")).strip():
                rows.append({
                    "date": format_date_str(row.get("next_action_date", "")),
                    "種別": "顧客次回対応",
                    "顧客名": row.get("company_name", ""),
                    "内容": row.get("next_action", ""),
                    "担当者": row.get("staff", ""),
                })

    if not activity_df.empty:
        for _, row in activity_df.iterrows():
            if str(row.get("next_action", "")).strip():
                rows.append({
                    "date": format_date_str(row.get("next_date", "")),
                    "種別": "活動フォロー",
                    "顧客名": row.get("customer_name", ""),
                    "内容": row.get("next_action", ""),
                    "担当者": row.get("staff", ""),
                })

    if not dm_df.empty:
        for _, row in dm_df.iterrows():
            if str(row.get("followup_done", "")) != "済":
                rows.append({
                    "date": format_date_str(row.get("followup_due_date", "")),
                    "種別": "DMフォロー",
                    "顧客名": row.get("customer_name", ""),
                    "内容": row.get("dm_type", ""),
                    "担当者": row.get("staff", ""),
                })

    todo_df = pd.DataFrame(rows)
    if not todo_df.empty:
        todo_df["date_sort"] = pd.to_datetime(todo_df["date"], errors="coerce")
        todo_df = todo_df.sort_values("date_sort").drop(columns=["date_sort"])
    return todo_df


def build_calendar_events(customers_df, activity_df, dm_df):
    events = []

    if not customers_df.empty:
        for _, row in customers_df.iterrows():
            next_d = parse_date_str(row.get("next_action_date", ""))
            if next_d and str(row.get("next_action", "")).strip():
                events.append({"date": next_d, "type": "todo", "label": f"{row.get('company_name','')}｜{row.get('next_action','')}"})

            renewal_month = safe_int(row.get("renewal_month", 0))
            if renewal_month > 0:
                renewal_date = date(date.today().year, renewal_month, 1)
                events.append({"date": renewal_date, "type": "renewal", "label": f"{row.get('company_name','')}｜更改月"})

    if not activity_df.empty:
        for _, row in activity_df.iterrows():
            d = parse_date_str(row.get("next_date", ""))
            if d and str(row.get("next_action", "")).strip():
                events.append({"date": d, "type": "visit", "label": f"{row.get('customer_name','')}｜{row.get('activity_type','')}"})

    if not dm_df.empty:
        for _, row in dm_df.iterrows():
            d = parse_date_str(row.get("followup_due_date", ""))
            if d:
                events.append({"date": d, "type": "dm", "label": f"{row.get('customer_name','')}｜{row.get('dm_type','')}"})

    return events


def build_dm_alerts(dm_df, activity_df):
    rows = []
    if dm_df.empty:
        return pd.DataFrame()

    for _, dm in dm_df.iterrows():
        send_d = parse_date_str(dm.get("send_date", ""))
        customer_name = str(dm.get("customer_name", ""))
        if not send_d or not customer_name:
            continue

        if str(dm.get("followup_done", "")) == "済":
            continue

        customer_acts = activity_df[activity_df["customer_name"] == customer_name].copy() if not activity_df.empty else pd.DataFrame()
        has_contact_after_dm = False

        if not customer_acts.empty:
            act_dates = pd.to_datetime(customer_acts["date"], errors="coerce").dropna()
            if not act_dates.empty:
                has_contact_after_dm = any(d.date() > send_d for d in act_dates)

        elapsed = (date.today() - send_d).days
        if (not has_contact_after_dm) and elapsed >= 7:
            level = "注意"
            if elapsed >= 30:
                level = "優先対応"
            elif elapsed >= 14:
                level = "要対応"

            rows.append({
                "顧客名": customer_name,
                "DM種類": dm.get("dm_type", ""),
                "発送日": send_d.strftime("%Y-%m-%d"),
                "経過日数": elapsed,
                "担当者": dm.get("staff", ""),
                "レベル": level,
            })
    return pd.DataFrame(rows)


def render_address_preview(row, mode="label"):
    postal = str(row.get("postal_code", "")).strip()
    address1 = str(row.get("address1", "")).strip()
    address2 = str(row.get("address2", "")).strip()
    company = str(row.get("company_name", "")).strip()
    dept = str(row.get("department_name", "")).strip()
    attn = str(row.get("attn_name", "")).strip()
    keisho = str(row.get("keisho", "")).strip()

    if mode == "label":
        return f"""〒{postal}
{address1}
{address2}
{company}
{dept}
{attn} {keisho}""".strip()

    return f"""〒 {postal}

{address1}
{address2}

{company}
{dept}
{attn} {keisho}""".strip()


def month_calendar_dates(target_date):
    first = target_date.replace(day=1)
    start = first - timedelta(days=first.weekday())
    dates = [start + timedelta(days=i) for i in range(42)]
    return [dates[i:i + 7] for i in range(0, 42, 7)]

# =========================================
# データロード
# =========================================
kpi_df = get_kpi_df()
targets_df = get_targets_df()
target = targets_df.iloc[0]
customers_df = get_customers_df()
activity_df = get_activity_df()
opportunities_df = get_opportunities_df()
insurance_status_df = get_insurance_status_df()
dm_history_df = get_dm_history_df()
customer_names = customers_df["company_name"].dropna().astype(str).tolist() if not customers_df.empty else []

# =========================================
# サイドバー
# =========================================
st.sidebar.title(APP_TITLE)
st.sidebar.write(f"ログイン中: {user_name}")
st.sidebar.write(user_email)

if st.sidebar.button("ログアウト"):
    st.logout()
    st.stop()

menu = st.sidebar.radio(
    "メニュー",
    [
        "スマホ簡易ホーム",
        "ダッシュボード",
        "バックアップ",
        "日次入力",
        "顧客管理",
        "顧客詳細",
        "訪問履歴",
        "案件管理",
        "保険加入状況",
        "DM発送履歴",
        "宛名印刷",
        "カレンダー",
        "ToDo一覧",
        "入力データ一覧",
        "月次集計",
        "目標設定",
        "5年予測",
    ],
)

st.title(APP_TITLE)
st.caption("SQLite高速版・Googleログイン対応・スマホ簡易ホーム付き")

# =========================================
# 画面
# =========================================
if menu == "スマホ簡易ホーム":
    st.subheader("スマホ簡易ホーム")

    st.markdown(
        f"""
<div class="small-card">
<b>スマホ確認用URL（同じWi-Fi内）</b><br>
http://{get_local_ip()}:8501
</div>
""",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric("顧客数", len(customers_df))
    with col2:
        st.metric("今日のToDo", len(build_todo_df(customers_df, activity_df, dm_history_df).query("date == @date.today().strftime('%Y-%m-%d')")) if not customers_df.empty else 0)

    st.write("### 今日の予定")
    todo_df = build_todo_df(customers_df, activity_df, dm_history_df)
    today_str = date.today().strftime("%Y-%m-%d")
    today_todo = todo_df[todo_df["date"] == today_str] if not todo_df.empty else pd.DataFrame()
    if today_todo.empty:
        st.info("今日は予定がありません。")
    else:
        st.dataframe(today_todo, use_container_width=True)

    st.write("### DM未接触アラート")
    dm_alerts = build_dm_alerts(dm_history_df, activity_df)
    if dm_alerts.empty:
        st.info("未接触アラートはありません。")
    else:
        st.dataframe(dm_alerts.head(10), use_container_width=True)

    st.write("### クイック活動入力")
    if not customer_names:
        st.info("先に顧客を登録してください。")
    else:
        quick_customer = st.selectbox("顧客", customer_names, key="quick_customer")
        quick_type = st.radio("活動", ["訪問", "TEL", "メール", "提案"], horizontal=True, key="quick_type")
        quick_result = st.radio("結果", ["見積依頼", "再訪問", "保留", "契約"], horizontal=True, key="quick_result")
        quick_memo = st.text_input("メモ", key="quick_memo")
        if st.button("クイック保存"):
            customer_row = customers_df[customers_df["company_name"] == quick_customer].iloc[0]
            insert_activity((
                str(date.today()),
                int(customer_row["customer_id"]),
                quick_customer,
                quick_type,
                user_name or "本人",
                "",
                quick_memo,
                "",
                str(date.today()),
                quick_result,
                "中",
            ))
            st.success("活動を保存しました。")
            st.rerun()

    st.write("### クイックDM登録")
    if customer_names:
        dm_customer = st.selectbox("顧客 ", customer_names, key="quick_dm_customer")
        dm_type = st.radio("DM種類", ["更改案内", "BCP案内", "南海トラフ対策", "生命保険案内"], horizontal=True, key="quick_dm_type")
        if st.button("DM送付を記録"):
            customer_row = customers_df[customers_df["company_name"] == dm_customer].iloc[0]
            insert_dm((
                str(date.today()),
                int(customer_row["customer_id"]),
                dm_customer,
                dm_type,
                dm_type,
                "",
                user_name or "本人",
                str(date.today() + timedelta(days=14)),
                "未対応",
                "反応なし",
            ))
            st.success("DM履歴を保存しました。")
            st.rerun()

elif menu == "バックアップ":
    st.subheader("バックアップ")

    st.write(f"本体DB保存先: {DB_PATH}")
    st.write(f"バックアップ保存先: {BACKUP_DIR}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("今すぐバックアップ作成"):
            backup_path = create_backup()
            st.success(f"バックアップ作成: {backup_path.name}")

    with col2:
        if DB_PATH.exists():
            with open(DB_PATH, "rb") as f:
                st.download_button(
                    "本体DBをダウンロード",
                    data=f.read(),
                    file_name="insudesk.db",
                    mime="application/octet-stream",
                )

    backups_df = list_backups()
    st.write("### バックアップ一覧")
    if backups_df.empty:
        st.info("まだバックアップはありません。")
    else:
        st.dataframe(backups_df[["filename", "size_kb", "updated"]], use_container_width=True)
        selected = st.selectbox("ダウンロードするバックアップ", backups_df["filename"].tolist())
        selected_path = BACKUP_DIR / selected
        if selected_path.exists():
            with open(selected_path, "rb") as f:
                st.download_button(
                    "選択したバックアップをダウンロード",
                    data=f.read(),
                    file_name=selected,
                    mime="application/octet-stream",
                )

elif menu == "ダッシュボード":
    monthly = to_monthly(kpi_df)

    if monthly.empty:
        st.info("まだデータがありません。先に日次入力をしてください。")
    else:
        latest_month = monthly["month"].iloc[-1]
        current = monthly[monthly["month"] == latest_month].iloc[0]

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("今月新規件数", f"{safe_int(current['new_cases'])}件")
        col2.metric("今月テレアポ", f"{safe_int(current['teleapo'])}件")
        col3.metric("今月訪問", f"{safe_int(current['visits'])}件")
        col4.metric("今月成約", f"{safe_int(current['contracts'])}件")
        col5.metric("保有収保", f"{safe_int(current['inforce_premium']):,}円")

        col6, col7, col8, col9 = st.columns(4)
        col6.metric("今月損保新規保険料", f"{safe_int(current['sonpo_premium']):,}円")
        col7.metric("今月損保手数料", f"{safe_int(current['sonpo_commission']):,}円")
        col8.metric("今月新規生保S", f"{safe_int(current['seiho_new_s']):,}円")
        col9.metric("今月生保手数料", f"{safe_int(current['seiho_commission']):,}円")

        dm_alerts = build_dm_alerts(dm_history_df, activity_df)
        if not dm_alerts.empty:
            st.write("### DM発送後 未接触アラート")
            st.dataframe(dm_alerts, use_container_width=True)

elif menu == "日次入力":
    st.subheader("日次入力")
    with st.form("daily_input_form"):
        input_date = st.date_input("日付", value=date.today())
        staff = st.text_input("担当者名", value=user_name or "本人")
        new_cases = st.number_input("新規件数", min_value=0, value=0)
        teleapo = st.number_input("テレアポ件数", min_value=0, value=0)
        visits = st.number_input("訪問件数", min_value=0, value=0)
        contracts = st.number_input("成約件数", min_value=0, value=0)
        sonpo_premium = st.number_input("損保新規保険料（円）", min_value=0, value=0, step=10000)
        sonpo_commission = st.number_input("損保手数料（円）", min_value=0, value=0, step=1000)
        seiho_new_s = st.number_input("新規生保S（円）", min_value=0, value=0, step=10000)
        seiho_commission = st.number_input("生保手数料（円）", min_value=0, value=0, step=1000)
        inforce_premium = st.number_input("保有収保（円）", min_value=0, value=0, step=10000)
        memo = st.text_input("備考", value="")
        submitted = st.form_submit_button("保存")

        if submitted:
            insert_kpi((
                str(input_date), staff, new_cases, teleapo, visits, contracts,
                sonpo_premium, sonpo_commission, seiho_new_s, seiho_commission,
                inforce_premium, memo
            ))
            st.success("保存しました。")
            st.rerun()

elif menu == "顧客管理":
    st.subheader("顧客管理")
    tab1, tab2, tab3 = st.tabs(["顧客追加", "顧客編集", "顧客一覧"])

    with tab1:
        category = st.radio("区分", ["法人", "個人"], horizontal=True)
        company_name = st.text_input("法人名 / 個人名")
        department_name = st.text_input("部署名")
        attn_name = st.text_input("宛名名")
        keisho = st.selectbox("敬称", KEISHO_OPTIONS)
        contact_name = st.text_input("法人担当者氏名 / 個人担当者氏名")
        rep_phone = st.text_input("法人代表電話")
        contact_phone = st.text_input("法人担当者電話番号")
        phone = st.text_input("その他電話番号")
        email = st.text_input("メール")
        postal_code = st.text_input("郵便番号")
        address1 = st.text_input("住所1")
        address2 = st.text_input("住所2")
        staff = st.text_input("自社担当者", value=user_name or "本人")
        status = st.selectbox("状態", ["見込", "提案中", "契約中", "失注", "既契約"])
        customer_rank = st.selectbox("顧客ランク", ["A", "B", "C"])
        insurance_types = st.multiselect("保険種類", ALL_INSURANCE_OPTIONS)
        sonpo_annual_premium = st.number_input("損保年間保険料（円）", min_value=0, value=0, step=10000)
        seiho_annual_premium = st.number_input("生保年間保険料（円）", min_value=0, value=0, step=10000)
        renewal_month = st.selectbox("更改月", list(range(0, 13)), format_func=lambda x: "未設定" if x == 0 else f"{x}月")

        if category == "法人":
            industry = st.selectbox("業種", INDUSTRY_OPTIONS)
            annual_sales = st.number_input("年商（円）", min_value=0, value=0, step=1000000)
            employee_count = st.number_input("従業員数", min_value=0, value=0)
            nankai_priority = st.selectbox("南海トラフ優先度", NANKAI_PRIORITY_OPTIONS)
            bcp_exists = st.selectbox("BCP策定有無", ["有", "無"])
            continuity_plan_applied = st.selectbox("事業継続強化計画申請有無", ["有", "無"])
        else:
            industry = ""
            annual_sales = 0
            employee_count = 0
            nankai_priority = ""
            bcp_exists = ""
            continuity_plan_applied = ""

        last_contact_date = st.date_input("最終接触日", value=date.today())
        next_action = st.text_input("次回アクション")
        next_action_date = st.date_input("次回予定日", value=date.today())
        memo = st.text_input("備考")

        if st.button("顧客を保存"):
            if not company_name.strip():
                st.error("法人名または個人名を入力してください。")
            else:
                insert_customer((
                    category, company_name, department_name, attn_name, keisho,
                    contact_name, rep_phone, contact_phone, phone, email,
                    postal_code, address1, address2, staff, status, customer_rank,
                    ",".join(insurance_types), industry, annual_sales, employee_count,
                    nankai_priority, bcp_exists, continuity_plan_applied,
                    sonpo_annual_premium, seiho_annual_premium, renewal_month,
                    str(last_contact_date), next_action, str(next_action_date), memo
                ))
                st.success("顧客を保存しました。")
                st.rerun()

    with tab2:
        if customers_df.empty:
            st.info("顧客データがありません。")
        else:
            edit_target = st.selectbox("編集する顧客を選択", customers_df["company_name"].tolist())
            row = customers_df[customers_df["company_name"] == edit_target].iloc[0]

            edit_company_name = st.text_input("法人名 / 個人名", value=row["company_name"])
            edit_staff = st.text_input("自社担当者", value=row["staff"])

            col1, col2 = st.columns(2)
            with col1:
                if st.button("顧客名と担当者を更新"):
                    values = (
                        row["category"], edit_company_name, row["department_name"], row["attn_name"], row["keisho"],
                        row["contact_name"], row["rep_phone"], row["contact_phone"], row["phone"], row["email"],
                        row["postal_code"], row["address1"], row["address2"], edit_staff, row["status"], row["customer_rank"],
                        row["insurance_types"], row["industry"], row["annual_sales"], row["employee_count"],
                        row["nankai_priority"], row["bcp_exists"], row["continuity_plan_applied"],
                        row["sonpo_annual_premium"], row["seiho_annual_premium"], row["renewal_month"],
                        row["last_contact_date"], row["next_action"], row["next_action_date"], row["memo"]
                    )
                    update_customer(int(row["customer_id"]), values)
                    st.success("更新しました。")
                    st.rerun()
            with col2:
                if st.button("この顧客を削除"):
                    delete_customer(int(row["customer_id"]))
                    st.success("削除しました。")
                    st.rerun()

    with tab3:
        if customers_df.empty:
            st.info("まだ顧客データがありません。")
        else:
            view_df = customers_df.copy()
            view_df["未接触日数"] = view_df["last_contact_date"].apply(days_since)
            st.dataframe(view_df, use_container_width=True)

elif menu == "顧客詳細":
    st.subheader("顧客詳細")
    if not customer_names:
        st.info("顧客データがありません。")
    else:
        selected_customer = st.selectbox("顧客を選択", customer_names)
        row = customers_df[customers_df["company_name"] == selected_customer].iloc[0]

        st.write("### 基本情報")
        st.dataframe(pd.DataFrame({
            "項目": ["区分", "会社名", "担当者", "担当電話", "代表電話", "メール", "住所"],
            "内容": [
                row["category"], row["company_name"], row["contact_name"], row["contact_phone"],
                row["rep_phone"], row["email"], f"{row['address1']} {row['address2']}"
            ],
        }), use_container_width=True)

elif menu == "訪問履歴":
    st.subheader("訪問履歴")
    if not customer_names:
        st.info("先に顧客を登録してください。")
    else:
        tab1, tab2 = st.tabs(["履歴追加", "履歴一覧"])
        with tab1:
            activity_customer = st.selectbox("顧客", customer_names)
            quick_type = st.radio("活動種別", ACTIVITY_TYPE_OPTIONS, horizontal=True)
            activity_date = st.date_input("日付", value=date.today())
            activity_staff = st.text_input("担当者", value=user_name or "本人")
            activity_insurance = st.selectbox("提案商品", [""] + ALL_INSURANCE_OPTIONS)
            activity_memo = st.text_input("内容メモ")
            activity_next_action = st.text_input("次回アクション")
            activity_next_date = st.date_input("次回予定日", value=date.today())
            activity_result = st.radio("成果", RESULT_OPTIONS, horizontal=True)
            activity_temp = st.radio("温度感", TEMPERATURE_OPTIONS, horizontal=True)

            if st.button("履歴を保存"):
                customer_row = customers_df[customers_df["company_name"] == activity_customer].iloc[0]
                insert_activity((
                    str(activity_date), int(customer_row["customer_id"]), activity_customer, quick_type,
                    activity_staff, activity_insurance, activity_memo, activity_next_action,
                    str(activity_next_date), activity_result, activity_temp
                ))
                st.success("訪問履歴を保存しました。")
                st.rerun()

        with tab2:
            if activity_df.empty:
                st.info("まだ履歴がありません。")
            else:
                st.dataframe(activity_df.sort_values("date", ascending=False), use_container_width=True)

elif menu == "案件管理":
    st.subheader("案件管理")
    if not customer_names:
        st.info("先に顧客を登録してください。")
    else:
        tab1, tab2 = st.tabs(["案件追加", "案件一覧"])
        with tab1:
            opp_customer = st.selectbox("顧客", customer_names)
            opp_insurance = st.selectbox("保険種類", ALL_INSURANCE_OPTIONS)
            opp_status = st.radio("ステータス", OPPORTUNITY_STATUS_OPTIONS, horizontal=True)
            opp_premium = st.number_input("見込保険料（円）", min_value=0, value=0, step=10000)
            opp_commission = st.number_input("見込手数料（円）", min_value=0, value=0, step=1000)
            opp_renewal_month = st.selectbox("更改月", list(range(0, 13)), format_func=lambda x: "未設定" if x == 0 else f"{x}月")
            opp_probability = st.slider("確度（%）", 0, 100, 50)
            opp_memo = st.text_input("備考")

            if st.button("案件を保存"):
                customer_row = customers_df[customers_df["company_name"] == opp_customer].iloc[0]
                insert_opportunity((
                    int(customer_row["customer_id"]), opp_customer, opp_insurance, opp_status,
                    opp_premium, opp_commission, opp_renewal_month, opp_probability, opp_memo
                ))
                st.success("案件を保存しました。")
                st.rerun()

        with tab2:
            if opportunities_df.empty:
                st.info("まだ案件がありません。")
            else:
                st.dataframe(opportunities_df, use_container_width=True)

elif menu == "保険加入状況":
    st.subheader("保険加入状況")
    if not customer_names:
        st.info("先に顧客を登録してください。")
    else:
        tab1, tab2 = st.tabs(["加入状況追加", "加入状況一覧"])
        with tab1:
            status_customer = st.selectbox("顧客", customer_names)
            status_insurance = st.selectbox("保険種類", ALL_INSURANCE_OPTIONS)
            status_progress = st.radio("進捗", INSURANCE_PROGRESS_OPTIONS, horizontal=True)
            status_memo = st.text_input("備考")

            if st.button("加入状況を保存"):
                customer_row = customers_df[customers_df["company_name"] == status_customer].iloc[0]
                insert_insurance_status((
                    int(customer_row["customer_id"]), status_customer, status_insurance, status_progress, status_memo
                ))
                st.success("加入状況を保存しました。")
                st.rerun()

        with tab2:
            if insurance_status_df.empty:
                st.info("まだ加入状況データがありません。")
            else:
                st.dataframe(insurance_status_df, use_container_width=True)

elif menu == "DM発送履歴":
    st.subheader("DM発送履歴")
    if not customer_names:
        st.info("先に顧客を登録してください。")
    else:
        tab1, tab2, tab3 = st.tabs(["DM追加", "DM一覧", "未接触アラート"])
        with tab1:
            dm_customer = st.selectbox("顧客", customer_names)
            dm_send_date = st.date_input("発送日", value=date.today())
            dm_type = st.radio("DM種類", DM_TYPE_OPTIONS, horizontal=True)
            dm_title = st.text_input("タイトル")
            dm_memo = st.text_input("メモ")
            dm_staff = st.text_input("担当者", value=user_name or "本人")
            dm_followup_due_date = st.date_input("フォロー予定日", value=date.today() + timedelta(days=14))
            dm_followup_done = st.selectbox("フォロー対応", ["未対応", "済"])
            dm_reaction = st.selectbox("反応", DM_REACTION_OPTIONS)

            if st.button("DM履歴を保存"):
                customer_row = customers_df[customers_df["company_name"] == dm_customer].iloc[0]
                insert_dm((
                    str(dm_send_date), int(customer_row["customer_id"]), dm_customer,
                    dm_type, dm_title, dm_memo, dm_staff,
                    str(dm_followup_due_date), dm_followup_done, dm_reaction
                ))
                st.success("DM履歴を保存しました。")
                st.rerun()

        with tab2:
            if dm_history_df.empty:
                st.info("DM履歴がありません。")
            else:
                st.dataframe(dm_history_df.sort_values("send_date", ascending=False), use_container_width=True)

        with tab3:
            alerts = build_dm_alerts(dm_history_df, activity_df)
            if alerts.empty:
                st.info("未接触アラートはありません。")
            else:
                st.dataframe(alerts.sort_values("経過日数", ascending=False), use_container_width=True)

elif menu == "宛名印刷":
    st.subheader("宛名印刷")
    if customers_df.empty:
        st.info("顧客データがありません。")
    else:
        mode = st.radio("印刷形式", ["A4ラベル", "ハガキ"], horizontal=True)
        count = st.number_input("表示件数", min_value=1, max_value=max(1, len(customers_df)), value=min(12, max(1, len(customers_df))))
        preview_df = customers_df.head(count).copy()

        if mode == "A4ラベル":
            cols_per_row = 3
            for start in range(0, len(preview_df), cols_per_row):
                cols = st.columns(cols_per_row)
                chunk = preview_df.iloc[start:start + cols_per_row]
                for i, (_, row) in enumerate(chunk.iterrows()):
                    with cols[i]:
                        st.text(render_address_preview(row, mode="label"))
        else:
            for _, row in preview_df.iterrows():
                st.text(render_address_preview(row, mode="postcard"))
                st.divider()

elif menu == "カレンダー":
    st.subheader("カレンダー")
    events = build_calendar_events(customers_df, activity_df, dm_history_df)
    view_mode = st.radio("表示切替", ["月表示", "週表示", "日表示"], horizontal=True)
    base_date = st.date_input("基準日", value=date.today())

    event_map = {}
    for e in events:
        event_map.setdefault(e["date"], []).append(e)

    if view_mode == "月表示":
        weeks = month_calendar_dates(base_date)
        weekday_names = ["月", "火", "水", "木", "金", "土", "日"]
        header_cols = st.columns(7)
        for i, name in enumerate(weekday_names):
            header_cols[i].markdown(f"**{name}**")

        for week in weeks:
            cols = st.columns(7)
            for i, d in enumerate(week):
                with cols[i]:
                    muted = "" if d.month == base_date.month else "calendar-muted"
                    st.markdown(f'<div class="calendar-day"><div class="{muted}"><b>{d.day}</b></div>', unsafe_allow_html=True)
                    for e in event_map.get(d, [])[:4]:
                        css = {
                            "visit": "event-visit",
                            "todo": "event-todo",
                            "dm": "event-dm",
                            "renewal": "event-renewal",
                            "alert": "event-alert",
                        }.get(e["type"], "event-todo")
                        st.markdown(f'<div class="event-box {css}">{e["label"]}</div>', unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

    elif view_mode == "週表示":
        start_week = base_date - timedelta(days=base_date.weekday())
        week_days = [start_week + timedelta(days=i) for i in range(7)]
        cols = st.columns(7)
        for i, d in enumerate(week_days):
            with cols[i]:
                st.markdown(f'<div class="calendar-day"><b>{d.month}/{d.day}</b>', unsafe_allow_html=True)
                day_events = event_map.get(d, [])
                if not day_events:
                    st.write("予定なし")
                else:
                    for e in day_events:
                        css = {
                            "visit": "event-visit",
                            "todo": "event-todo",
                            "dm": "event-dm",
                            "renewal": "event-renewal",
                            "alert": "event-alert",
                        }.get(e["type"], "event-todo")
                        st.markdown(f'<div class="event-box {css}">{e["label"]}</div>', unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

    else:
        st.write(f"### {base_date.strftime('%Y-%m-%d')} の予定")
        day_events = event_map.get(base_date, [])
        if not day_events:
            st.info("予定はありません。")
        else:
            st.dataframe(pd.DataFrame(day_events)[["type", "label"]].rename(columns={"type": "種別", "label": "内容"}), use_container_width=True)

elif menu == "ToDo一覧":
    st.subheader("ToDo一覧")
    todo_df = build_todo_df(customers_df, activity_df, dm_history_df)
    if todo_df.empty:
        st.info("未対応の予定はありません。")
    else:
        st.dataframe(todo_df, use_container_width=True)

elif menu == "入力データ一覧":
    st.subheader("入力データ一覧")
    if kpi_df.empty:
        st.info("まだデータがありません。")
    else:
        st.dataframe(kpi_df, use_container_width=True)

elif menu == "月次集計":
    st.subheader("月次集計")
    monthly = to_monthly(kpi_df)
    if monthly.empty:
        st.info("まだデータがありません。")
    else:
        st.dataframe(monthly, use_container_width=True)

elif menu == "目標設定":
    st.subheader("目標設定")
    with st.form("target_form"):
        new_cases_target = st.number_input("月間新規件数目標", min_value=0, value=safe_int(target["new_cases_target"]))
        teleapo_target = st.number_input("月間テレアポ目標", min_value=0, value=safe_int(target["teleapo_target"]))
        visits_target = st.number_input("月間訪問目標", min_value=0, value=safe_int(target["visits_target"]))
        contracts_target = st.number_input("月間成約目標", min_value=0, value=safe_int(target["contracts_target"]))
        sonpo_premium_target = st.number_input("月間損保新規保険料目標（円）", min_value=0, value=safe_int(target["sonpo_premium_target"]), step=10000)
        sonpo_commission_target = st.number_input("月間損保手数料目標（円）", min_value=0, value=safe_int(target["sonpo_commission_target"]), step=1000)
        seiho_new_s_target = st.number_input("月間新規生保S目標（円）", min_value=0, value=safe_int(target["seiho_new_s_target"]), step=10000)
        seiho_commission_target = st.number_input("月間生保手数料目標（円）", min_value=0, value=safe_int(target["seiho_commission_target"]), step=1000)
        inforce_premium_target = st.number_input("保有収保目標（円）", min_value=0, value=safe_int(target["inforce_premium_target"]), step=10000)
        submitted = st.form_submit_button("目標を保存")

        if submitted:
            update_targets((
                new_cases_target, teleapo_target, visits_target, contracts_target,
                sonpo_premium_target, sonpo_commission_target,
                seiho_new_s_target, seiho_commission_target, inforce_premium_target
            ))
            st.success("目標を保存しました。")
            st.rerun()

elif menu == "5年予測":
    st.subheader("5年予測")
    latest_inforce = safe_int(kpi_df["inforce_premium"].max()) if not kpi_df.empty else 0
    total_new_cases = safe_int(kpi_df["new_cases"].sum()) if not kpi_df.empty else 0
    total_sonpo_premium = safe_int(kpi_df["sonpo_premium"].sum()) if not kpi_df.empty else 0
    total_sonpo_commission = safe_int(kpi_df["sonpo_commission"].sum()) if not kpi_df.empty else 0
    avg_premium_per_case_default = int(total_sonpo_premium / total_new_cases) if total_new_cases > 0 else 0
    avg_commission_per_case_default = int(total_sonpo_commission / total_new_cases) if total_new_cases > 0 else 0

    col1, col2 = st.columns(2)
    with col1:
        current_inforce = st.number_input("現在の保有収保（円）", min_value=0, value=latest_inforce, step=10000)
        monthly_new_cases = st.number_input("毎月の新規件数", min_value=0, value=2)
        avg_premium_per_case = st.number_input("1件あたり平均保険料（円）", min_value=0, value=avg_premium_per_case_default, step=10000)
    with col2:
        avg_commission_per_case = st.number_input("1件あたり平均損保手数料（円）", min_value=0, value=avg_commission_per_case_default, step=1000)
        annual_retention_rate = st.number_input("継続率（%）", min_value=0.0, max_value=100.0, value=90.0, step=0.5)

    forecast_df = make_forecast(current_inforce, monthly_new_cases, avg_premium_per_case, avg_commission_per_case, annual_retention_rate)
    st.dataframe(forecast_df, use_container_width=True)
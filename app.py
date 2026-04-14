import io
import shutil
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st

APP_TITLE = "InsuDesk"
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "insudesk.db"
BACKUP_DIR = BASE_DIR / "backup"
IMAGE_DIR = BASE_DIR / "customer_images"

BACKUP_DIR.mkdir(exist_ok=True)
IMAGE_DIR.mkdir(exist_ok=True)

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

LIFE_INSURANCE_OPTIONS = ["第一分野", "第三分野"]

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
RESULT_CODE_OPTIONS = [
    "A 成立",
    "B 見積提出",
    "C 新規見込",
    "D 断り",
    "K 更新案内",
    "G 事故対応",
]

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
    html, body, [class*="css"], [class*="st-"], .main, .block-container {
        font-family: "Meiryo", "メイリオ", sans-serif !important;
    }
    .section-box {
        border: 1px solid #dddddd;
        border-radius: 12px;
        padding: 14px;
        margin-bottom: 14px;
        background: #fafafa;
    }
    .visit-box {
        border: 2px solid #0f766e;
        border-radius: 14px;
        padding: 14px;
        margin-bottom: 16px;
        background: #ecfeff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def exec_sql(sql: str, params=(), fetch=False):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return rows


def load_df(sql: str, params=()):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


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


def days_since(value):
    d = parse_date_str(value)
    if not d:
        return None
    return (date.today() - d).days


def calc_rate(numerator, denominator):
    if denominator in [0, None]:
        return 0.0
    return round((numerator / denominator) * 100, 1)


def format_currency(x):
    return f"{safe_int(x):,}円"


def init_db():
    conn = get_conn()
    cur = conn.cursor()

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
        website_url TEXT,
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
        memo TEXT,
        business_card_image TEXT,
        last_contact_date TEXT,
        next_action TEXT,
        next_action_date TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS kpi_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        company_name TEXT,
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
        result_code TEXT,
        insurance_type TEXT,
        carrier_type TEXT,
        renewal_month INTEGER DEFAULT 0,
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS todos (
        todo_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        task_name TEXT,
        due_date TEXT,
        status TEXT,
        memo TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS visit_logs (
        visit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        company_name TEXT,
        visit_start TEXT,
        visit_end TEXT,
        duration_minutes INTEGER DEFAULT 0,
        result_code TEXT,
        memo TEXT,
        insurance_type TEXT,
        carrier_type TEXT,
        renewal_month INTEGER DEFAULT 0,
        created_at TEXT
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

    conn.commit()
    conn.close()


init_db()


def get_customers_df():
    return load_df("SELECT * FROM customers ORDER BY customer_id DESC")


def get_kpi_df():
    return load_df("SELECT * FROM kpi_data ORDER BY id DESC")


def get_activity_df():
    return load_df("SELECT * FROM activity_log ORDER BY activity_id DESC")


def get_opportunities_df():
    return load_df("SELECT * FROM opportunities ORDER BY opportunity_id DESC")


def get_insurance_status_df():
    return load_df("SELECT * FROM insurance_status ORDER BY status_id DESC")


def get_dm_history_df():
    return load_df("SELECT * FROM dm_history ORDER BY dm_id DESC")


def get_todos_df():
    return load_df("SELECT * FROM todos ORDER BY due_date ASC, todo_id DESC")


def get_visit_logs_df():
    return load_df("SELECT * FROM visit_logs ORDER BY visit_id DESC")


def get_targets_row():
    df = load_df("SELECT * FROM targets WHERE id=1")
    return df.iloc[0]


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


def build_todo_df(customers_df, activity_df, dm_df, todos_df):
    rows = []

    if not customers_df.empty:
        for _, row in customers_df.iterrows():
            if str(row.get("next_action", "")).strip():
                rows.append({
                    "date": str(row.get("next_action_date", "")),
                    "種別": "顧客次回対応",
                    "顧客名": row.get("company_name", ""),
                    "内容": row.get("next_action", ""),
                    "担当者": row.get("staff", ""),
                })

    if not activity_df.empty:
        for _, row in activity_df.iterrows():
            if str(row.get("next_action", "")).strip():
                rows.append({
                    "date": str(row.get("next_date", "")),
                    "種別": "活動フォロー",
                    "顧客名": row.get("customer_name", ""),
                    "内容": row.get("next_action", ""),
                    "担当者": row.get("staff", ""),
                })

    if not dm_df.empty:
        for _, row in dm_df.iterrows():
            if str(row.get("followup_done", "")) != "済":
                rows.append({
                    "date": str(row.get("followup_due_date", "")),
                    "種別": "DMフォロー",
                    "顧客名": row.get("customer_name", ""),
                    "内容": row.get("dm_type", ""),
                    "担当者": row.get("staff", ""),
                })

    if not todos_df.empty:
        for _, row in todos_df.iterrows():
            if str(row.get("status", "")) != "完了":
                rows.append({
                    "date": str(row.get("due_date", "")),
                    "種別": "ToDo",
                    "顧客名": row.get("company_name", ""),
                    "内容": row.get("task_name", ""),
                    "担当者": "",
                })

    todo_df = pd.DataFrame(rows)
    if not todo_df.empty:
        todo_df["date_sort"] = pd.to_datetime(todo_df["date"], errors="coerce")
        todo_df = todo_df.sort_values("date_sort").drop(columns=["date_sort"])
    return todo_df


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


def build_calendar_events(customers_df, activity_df, dm_df, todos_df):
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

    if not todos_df.empty:
        for _, row in todos_df.iterrows():
            d = parse_date_str(row.get("due_date", ""))
            if d and str(row.get("status", "")) != "完了":
                events.append({"date": d, "type": "todo", "label": f"{row.get('company_name','')}｜{row.get('task_name','')}"})

    return events


def month_calendar_dates(target_date):
    first = target_date.replace(day=1)
    start = first - timedelta(days=first.weekday())
    dates = [start + timedelta(days=i) for i in range(42)]
    return [dates[i:i + 7] for i in range(0, 42, 7)]


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


def backup_to_local():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"insudesk_backup_{ts}.db"
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def list_local_backups():
    files = sorted(BACKUP_DIR.glob("*.db"), reverse=True)
    rows = []
    for f in files:
        stat = f.stat()
        rows.append({
            "filename": f.name,
            "size_kb": round(stat.st_size / 1024, 1),
            "updated": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def insert_customer(data):
    exec_sql("""
        INSERT INTO customers (
            category, company_name, department_name, attn_name, keisho,
            contact_name, rep_phone, contact_phone, phone, email,
            postal_code, address1, address2, website_url,
            staff, status, customer_rank, insurance_types,
            industry, annual_sales, employee_count, nankai_priority,
            bcp_exists, continuity_plan_applied,
            sonpo_annual_premium, seiho_annual_premium, renewal_month,
            memo, business_card_image,
            last_contact_date, next_action, next_action_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def update_customer(customer_id, data):
    exec_sql("""
        UPDATE customers SET
            category=?, company_name=?, department_name=?, attn_name=?, keisho=?,
            contact_name=?, rep_phone=?, contact_phone=?, phone=?, email=?,
            postal_code=?, address1=?, address2=?, website_url=?,
            staff=?, status=?, customer_rank=?, insurance_types=?,
            industry=?, annual_sales=?, employee_count=?, nankai_priority=?,
            bcp_exists=?, continuity_plan_applied=?,
            sonpo_annual_premium=?, seiho_annual_premium=?, renewal_month=?,
            memo=?, business_card_image=?,
            last_contact_date=?, next_action=?, next_action_date=?
        WHERE customer_id=?
    """, data + (customer_id,))


def delete_customer(customer_id):
    exec_sql("DELETE FROM customers WHERE customer_id=?", (customer_id,))


def insert_kpi(data):
    exec_sql("""
        INSERT INTO kpi_data (
            date, company_name, staff, new_cases, teleapo, visits, contracts,
            sonpo_premium, sonpo_commission, seiho_new_s, seiho_commission,
            inforce_premium, result_code, insurance_type, carrier_type,
            renewal_month, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def update_targets(data):
    exec_sql("""
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
    """, data)


def insert_activity(data):
    exec_sql("""
        INSERT INTO activity_log (
            date, customer_id, customer_name, activity_type, staff,
            insurance_type, memo, next_action, next_date, result, temperature
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def insert_opportunity(data):
    exec_sql("""
        INSERT INTO opportunities (
            customer_id, customer_name, insurance_type, status,
            estimated_premium, estimated_commission, renewal_month, probability, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def insert_insurance_status(data):
    exec_sql("""
        INSERT INTO insurance_status (
            customer_id, customer_name, insurance_type, progress, memo
        ) VALUES (?, ?, ?, ?, ?)
    """, data)


def insert_dm(data):
    exec_sql("""
        INSERT INTO dm_history (
            send_date, customer_id, customer_name, dm_type, title,
            memo, staff, followup_due_date, followup_done, reaction
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def insert_todo(data):
    exec_sql("""
        INSERT INTO todos (
            company_name, task_name, due_date, status, memo, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, data)


def complete_todo(todo_id):
    exec_sql("UPDATE todos SET status='完了' WHERE todo_id=?", (todo_id,))


def insert_visit_start(customer_id, company_name):
    exec_sql("""
        INSERT INTO visit_logs (
            customer_id, company_name, visit_start, visit_end,
            duration_minutes, result_code, memo, insurance_type,
            carrier_type, renewal_month, created_at
        ) VALUES (?, ?, ?, NULL, 0, '', '', '', '', 0, ?)
    """, (
        customer_id,
        company_name,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ))


def get_open_visit(customer_id):
    rows = exec_sql("""
        SELECT * FROM visit_logs
        WHERE customer_id=? AND (visit_end IS NULL OR visit_end='')
        ORDER BY visit_id DESC LIMIT 1
    """, (customer_id,), fetch=True)
    return rows[0] if rows else None


def finish_visit(customer_id, result_code, memo, insurance_type, carrier_type, renewal_month):
    open_visit = get_open_visit(customer_id)
    if not open_visit:
        return None

    visit_id = open_visit[0]
    start_str = open_visit[3]
    start_dt = pd.to_datetime(start_str)
    end_dt = datetime.now()
    duration = int((end_dt - start_dt).total_seconds() / 60)

    exec_sql("""
        UPDATE visit_logs
        SET visit_end=?, duration_minutes=?, result_code=?, memo=?,
            insurance_type=?, carrier_type=?, renewal_month=?
        WHERE visit_id=?
    """, (
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        duration,
        result_code,
        memo,
        insurance_type,
        carrier_type,
        renewal_month,
        visit_id
    ))
    return {
        "visit_id": visit_id,
        "duration_minutes": duration,
        "start": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "end": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }


customers_df = get_customers_df()
kpi_df = get_kpi_df()
activity_df = get_activity_df()
opportunities_df = get_opportunities_df()
insurance_status_df = get_insurance_status_df()
dm_history_df = get_dm_history_df()
todos_df = get_todos_df()
visit_logs_df = get_visit_logs_df()
target = get_targets_row()

customer_names = customers_df["company_name"].dropna().astype(str).tolist() if not customers_df.empty else []

if "menu" not in st.session_state:
    st.session_state["menu"] = "カレンダー"

if "sales_from_visit" not in st.session_state:
    st.session_state["sales_from_visit"] = False

user_name = "本人"

st.sidebar.title(APP_TITLE)
st.sidebar.caption(f"ログイン中: {user_name}")

for item in ["カレンダー", "ToDo一覧"]:
    if st.sidebar.button(item, use_container_width=True, key=f"m_{item}"):
        st.session_state["menu"] = item
st.sidebar.markdown("---")

for item in ["顧客管理", "顧客詳細"]:
    if st.sidebar.button(item, use_container_width=True, key=f"m_{item}"):
        st.session_state["menu"] = item
st.sidebar.markdown("---")

for item in ["日次入力", "訪問履歴", "案件管理", "DM発送履歴"]:
    if st.sidebar.button(item, use_container_width=True, key=f"m_{item}"):
        st.session_state["menu"] = item
st.sidebar.markdown("---")

for item in ["保険加入状況", "宛名印刷"]:
    if st.sidebar.button(item, use_container_width=True, key=f"m_{item}"):
        st.session_state["menu"] = item
st.sidebar.markdown("---")

for item in ["ダッシュボード", "月次集計", "目標設定", "5年予測"]:
    if st.sidebar.button(item, use_container_width=True, key=f"m_{item}"):
        st.session_state["menu"] = item
st.sidebar.markdown("---")

for item in ["バックアップ", "スマホ簡易ホーム"]:
    if st.sidebar.button(item, use_container_width=True, key=f"m_{item}"):
        st.session_state["menu"] = item

menu = st.session_state["menu"]

st.title(APP_TITLE)
st.caption(f"現在メニュー: {menu}")

if menu == "スマホ簡易ホーム":
    st.subheader("スマホ簡易ホーム")

    today_todo = build_todo_df(customers_df, activity_df, dm_history_df, todos_df)
    today_str = date.today().strftime("%Y-%m-%d")
    today_only = today_todo[today_todo["date"] == today_str] if not today_todo.empty else pd.DataFrame()

    c1, c2 = st.columns(2)
    c1.metric("顧客数", len(customers_df))
    c2.metric("今日のToDo", len(today_only))

    st.write("### 今日の予定")
    if today_only.empty:
        st.info("今日は予定がありません。")
    else:
        st.dataframe(today_only, use_container_width=True)

    st.write("### DM未接触アラート")
    dm_alerts = build_dm_alerts(dm_history_df, activity_df)
    if dm_alerts.empty:
        st.info("未接触アラートはありません。")
    else:
        st.dataframe(dm_alerts.head(10), use_container_width=True)

elif menu == "バックアップ":
    st.subheader("バックアップ")
    st.info("Google Drive自動保存はいったん外し、ローカル保存 + DBダウンロードの安定版です。")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("今すぐローカルバックアップ作成", use_container_width=True):
            try:
                backup_path = backup_to_local()
                st.success(f"ローカル保存: {backup_path.name}")
            except Exception as e:
                st.error(f"バックアップ失敗: {e}")

    with c2:
        if DB_PATH.exists():
            with open(DB_PATH, "rb") as f:
                st.download_button(
                    "DBをダウンロード",
                    data=f.read(),
                    file_name=f"insudesk_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.db",
                    mime="application/octet-stream",
                    use_container_width=True,
                )

    backups_df = list_local_backups()
    st.write("### ローカルバックアップ一覧")
    if backups_df.empty:
        st.info("ローカルバックアップはありません。")
    else:
        st.dataframe(backups_df, use_container_width=True)

elif menu == "カレンダー":
    st.subheader("カレンダー")
    events = build_calendar_events(customers_df, activity_df, dm_history_df, todos_df)
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
                    muted = "color:#999;" if d.month != base_date.month else ""
                    st.markdown(f'<div class="section-box"><div style="{muted}"><b>{d.day}</b></div>', unsafe_allow_html=True)
                    for e in event_map.get(d, [])[:4]:
                        st.write(f"• {e['label']}")
                    st.markdown("</div>", unsafe_allow_html=True)

    elif view_mode == "週表示":
        start_week = base_date - timedelta(days=base_date.weekday())
        week_days = [start_week + timedelta(days=i) for i in range(7)]
        cols = st.columns(7)
        for i, d in enumerate(week_days):
            with cols[i]:
                st.markdown(f'<div class="section-box"><b>{d.month}/{d.day}</b>', unsafe_allow_html=True)
                day_events = event_map.get(d, [])
                if not day_events:
                    st.write("予定なし")
                else:
                    for e in day_events:
                        st.write(f"• {e['label']}")
                st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.write(f"### {base_date.strftime('%Y-%m-%d')} の予定")
        day_events = event_map.get(base_date, [])
        if not day_events:
            st.info("予定はありません。")
        else:
            st.dataframe(pd.DataFrame(day_events), use_container_width=True)

elif menu == "ToDo一覧":
    st.subheader("ToDo一覧")
    todo_df = build_todo_df(customers_df, activity_df, dm_history_df, todos_df)
    if todo_df.empty:
        st.info("未対応の予定はありません。")
    else:
        st.dataframe(todo_df, use_container_width=True)

    st.write("### 手動ToDo")
    with st.form("todo_form"):
        company_name = st.text_input("会社名")
        task_name = st.text_input("タスク名")
        due_date = st.date_input("期限", value=date.today() + timedelta(days=7))
        memo = st.text_input("メモ")
        submitted = st.form_submit_button("ToDo保存")
        if submitted:
            insert_todo((
                company_name,
                task_name,
                str(due_date),
                "未対応",
                memo,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            st.success("ToDoを保存しました。")
            st.rerun()

    if not todos_df.empty:
        st.write("### ToDo管理")
        target_todo = st.selectbox(
            "完了にするToDo",
            options=todos_df["todo_id"].tolist(),
            format_func=lambda x: f"{todos_df[todos_df['todo_id'] == x].iloc[0]['company_name']}｜{todos_df[todos_df['todo_id'] == x].iloc[0]['task_name']}"
        )
        if st.button("選択ToDoを完了"):
            complete_todo(int(target_todo))
            st.success("完了にしました。")
            st.rerun()

elif menu == "顧客管理":
    st.subheader("顧客管理")
    tab1, tab2, tab3 = st.tabs(["顧客追加", "顧客編集", "顧客一覧"])

    with tab1:
        business_card_file = st.file_uploader(
            "名刺画像アップロード",
            type=["png", "jpg", "jpeg", "webp"],
            key="business_card_upload"
        )
        zoom_preview = st.slider("名刺プレビュー倍率", 50, 200, 100, 10)
        if business_card_file is not None:
            st.image(business_card_file, caption="名刺プレビュー", width=int(320 * zoom_preview / 100))

        category = st.radio("区分", ["法人", "個人"], horizontal=True)
        company_name = st.text_input("法人名 / 個人名")
        department_name = st.text_input("部署名")
        attn_name = st.text_input("宛名名")
        keisho = st.selectbox("敬称", KEISHO_OPTIONS)
        contact_name = st.text_input("担当者氏名")
        rep_phone = st.text_input("法人代表電話")
        contact_phone = st.text_input("担当者電話番号")
        phone = st.text_input("その他電話番号")
        email = st.text_input("メール")
        postal_code = st.text_input("郵便番号")
        address1 = st.text_input("住所1")
        address2 = st.text_input("住所2")
        website_url = st.text_input("会社HP / Webサイト", placeholder="https://...")
        staff = st.text_input("自社担当者", value=user_name)
        status = st.selectbox("状態", ["見込", "提案中", "契約中", "失注", "既契約"])
        customer_rank = st.selectbox("顧客ランク", ["A", "B", "C"])
        insurance_types = st.multiselect("保険種類", ALL_INSURANCE_OPTIONS)
        sonpo_annual_premium = st.number_input("損保年間保険料（円）", min_value=0, value=0, step=10000)
        seiho_annual_premium = st.number_input("生保年間保険料（円）", min_value=0, value=0, step=10000)
        renewal_month = st.selectbox("更新月", list(range(0, 13)), format_func=lambda x: "未設定" if x == 0 else f"{x}月")

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

        memo = st.text_area(
            "備考 / OneNoteリンク",
            height=100,
            placeholder="面談メモや OneNote URL を貼り付け"
        )

        last_contact_date = st.date_input("最終接触日", value=date.today())
        next_action = st.text_input("次回アクション")
        next_action_date = st.date_input("次回予定日", value=date.today())

        if st.button("顧客を保存", use_container_width=True):
            if not company_name.strip():
                st.error("法人名または個人名を入力してください。")
            else:
                business_card_path = ""
                if business_card_file is not None:
                    ext = Path(business_card_file.name).suffix.lower()
                    safe_name = company_name.strip().replace(" ", "_").replace("/", "_")
                    image_path = IMAGE_DIR / f"{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
                    with open(image_path, "wb") as f:
                        f.write(business_card_file.getbuffer())
                    business_card_path = str(image_path)

                insert_customer((
                    category, company_name, department_name, attn_name, keisho,
                    contact_name, rep_phone, contact_phone, phone, email,
                    postal_code, address1, address2, website_url,
                    staff, status, customer_rank, ",".join(insurance_types),
                    industry, annual_sales, employee_count, nankai_priority,
                    bcp_exists, continuity_plan_applied,
                    sonpo_annual_premium, seiho_annual_premium, renewal_month,
                    memo, business_card_path,
                    str(last_contact_date), next_action, str(next_action_date)
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

            if st.button("顧客名と担当者を更新", use_container_width=True):
                data = (
                    row["category"], edit_company_name, row["department_name"], row["attn_name"], row["keisho"],
                    row["contact_name"], row["rep_phone"], row["contact_phone"], row["phone"], row["email"],
                    row["postal_code"], row["address1"], row["address2"], row["website_url"],
                    edit_staff, row["status"], row["customer_rank"], row["insurance_types"],
                    row["industry"], row["annual_sales"], row["employee_count"], row["nankai_priority"],
                    row["bcp_exists"], row["continuity_plan_applied"],
                    row["sonpo_annual_premium"], row["seiho_annual_premium"], row["renewal_month"],
                    row["memo"], row["business_card_image"],
                    row["last_contact_date"], row["next_action"], row["next_action_date"],
                )
                update_customer(int(row["customer_id"]), data)
                st.success("更新しました。")
                st.rerun()

            if st.button("この顧客を削除", use_container_width=True):
                delete_customer(int(row["customer_id"]))
                st.success("削除しました。")
                st.rerun()

    with tab3:
        if customers_df.empty:
            st.info("まだ顧客データがありません。")
        else:
            view_df = customers_df.copy()
            view_df["未接触日数"] = view_df["last_contact_date"].apply(days_since)
            view_df["名刺画像"] = view_df["business_card_image"].apply(lambda x: "あり" if str(x).strip() else "")
            st.dataframe(view_df[[
                "company_name", "category", "contact_name", "rep_phone", "contact_phone",
                "renewal_month", "website_url", "名刺画像", "未接触日数"
            ]], use_container_width=True)

elif menu == "顧客詳細":
    st.subheader("顧客詳細")
    if not customer_names:
        st.info("顧客データがありません。")
    else:
        selected_customer = st.selectbox("顧客を選択", customer_names)
        row = customers_df[customers_df["company_name"] == selected_customer].iloc[0]
        customer_id = int(row["customer_id"])

        st.markdown('<div class="visit-box">', unsafe_allow_html=True)
        st.markdown(f"## {row['company_name']}")
        st.info(f"現在表示中: {row['company_name']}")

        result_code = st.radio(
            "今回の商談結果",
            RESULT_CODE_OPTIONS,
            horizontal=True,
            key="visit_result_code"
        )

        c0, c1, c2 = st.columns([1.2, 1.2, 1])
        with c0:
            if st.button("🚗 訪問開始", use_container_width=True):
                insert_visit_start(customer_id, row["company_name"])
                st.success("訪問開始を記録しました。")
                st.rerun()

        with c1:
            finish_memo = st.session_state.get("visit_finish_memo", "")
            insurance_type_choice = st.session_state.get("visit_insurance_type", "")
            carrier_type_choice = st.session_state.get("visit_carrier_type", "AIG")
            renewal_month_choice = st.session_state.get("visit_renewal_month", safe_int(row.get("renewal_month", 0)))

            finish_memo = st.text_input("終了メモ", value=finish_memo, key="visit_finish_memo")
            insurance_type_choice = st.selectbox(
                "保険種類",
                [""] + ALL_INSURANCE_OPTIONS,
                index=([""] + ALL_INSURANCE_OPTIONS).index(insurance_type_choice) if insurance_type_choice in [""] + ALL_INSURANCE_OPTIONS else 0,
                key="visit_insurance_type",
            )
            carrier_type_choice = st.selectbox(
                "AIG / 他社",
                ["AIG", "他社"],
                index=0 if carrier_type_choice == "AIG" else 1,
                key="visit_carrier_type",
            )
            renewal_month_choice = st.selectbox(
                "更新月",
                list(range(0, 13)),
                index=renewal_month_choice if 0 <= safe_int(renewal_month_choice) <= 12 else 0,
                format_func=lambda x: "未設定" if x == 0 else f"{x}月",
                key="visit_renewal_month",
            )

            if st.button("🏁 訪問終了", use_container_width=True):
                result = finish_visit(
                    customer_id=customer_id,
                    result_code=result_code,
                    memo=finish_memo,
                    insurance_type=insurance_type_choice,
                    carrier_type=carrier_type_choice,
                    renewal_month=renewal_month_choice,
                )
                if result is None:
                    st.error("訪問開始が未記録です。先に訪問開始を押してください。")
                else:
                    st.success(f"訪問終了を記録しました（滞在 {result['duration_minutes']}分）")

                    if result_code == "A 成立":
                        st.session_state["sales_from_visit"] = True
                        st.session_state["sales_company"] = row["company_name"]
                        st.session_state["sales_insurance_type"] = insurance_type_choice
                        st.session_state["sales_carrier_type"] = carrier_type_choice
                        st.session_state["sales_renewal_month"] = renewal_month_choice
                        st.session_state["menu"] = "日次入力"
                        st.rerun()

                    elif result_code == "C 新規見込":
                        next_date = (date.today() + timedelta(days=7)).strftime("%Y-%m-%d")
                        insert_todo((
                            row["company_name"],
                            "新規見込フォロー",
                            next_date,
                            "未対応",
                            "訪問終了から自動作成",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ))
                        st.success(f"7日後ToDoを作成しました: {next_date}")

        with c2:
            if st.button("📝 訪問メモ登録", use_container_width=True):
                st.session_state["menu"] = "訪問履歴"
                st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

        col_left, col_right = st.columns([1.2, 1])

        with col_left:
            st.write("### 基本情報")
            rep_phone = str(row.get("rep_phone", "")).strip()
            contact_phone = str(row.get("contact_phone", "")).strip()
            other_phone = str(row.get("phone", "")).strip()
            address1 = str(row.get("address1", "")).strip()
            website_url = str(row.get("website_url", "")).strip()
            memo_text = str(row.get("memo", "")).strip()

            st.write(f"区分: {row['category']}")
            st.write(f"担当者: {row['contact_name']}")
            st.write(f"メール: {row['email']}")

            st.write("### 📞 電話")
            if rep_phone:
                st.markdown(f"[📞 代表電話: {rep_phone}](tel:{rep_phone.replace('-', '')})")
            if contact_phone:
                st.markdown(f"[📞 担当者電話: {contact_phone}](tel:{contact_phone.replace('-', '')})")
            if other_phone:
                st.markdown(f"[📞 その他電話: {other_phone}](tel:{other_phone.replace('-', '')})")

            st.write("### 📍 所在地")
            if address1:
                map_url = f"https://www.google.com/maps/search/{quote_plus(address1)}"
                route_url = f"https://www.google.com/maps/dir/?api=1&destination={quote_plus(address1)}"
                cm1, cm2 = st.columns(2)
                with cm1:
                    st.link_button("📍 地図を見る", map_url, use_container_width=True)
                with cm2:
                    st.link_button("🚗 ルート案内", route_url, use_container_width=True)
                st.caption(address1)
                if str(row.get("address2", "")).strip():
                    st.caption(str(row.get("address2", "")).strip())
            else:
                st.info("住所未登録")

            st.write("### 🌐 Web")
            if website_url:
                st.link_button("🌐 会社HPを見る", website_url, use_container_width=True)
            else:
                st.info("HP未登録")

            st.write("### 📘 備考 / OneNote")
            if memo_text:
                if memo_text.startswith("http") or memo_text.startswith("onenote:"):
                    st.link_button("📘 OneNoteを開く", memo_text, use_container_width=True)
                    st.code(memo_text)
                else:
                    st.write(memo_text)
            else:
                st.info("備考は未登録です。")

        with col_right:
            st.write("### 名刺画像")
            card_path = str(row.get("business_card_image", "")).strip()
            if card_path and Path(card_path).exists():
                detail_zoom = st.slider("名刺表示倍率", 50, 300, 100, 10, key="detail_card_zoom")
                st.image(card_path, caption="保存済み名刺画像", width=int(420 * detail_zoom / 100))
            else:
                st.info("名刺画像は未登録です。")

        st.write("### 訪問履歴")
        customer_visit_logs = visit_logs_df[visit_logs_df["customer_id"] == customer_id] if not visit_logs_df.empty else pd.DataFrame()
        if customer_visit_logs.empty:
            st.info("訪問履歴なし")
        else:
            st.dataframe(customer_visit_logs, use_container_width=True)

elif menu == "日次入力":
    st.subheader("日次入力")

    if st.session_state.get("sales_from_visit"):
        st.success(f"成立案件: {st.session_state.get('sales_company', '')}")

    with st.form("daily_input_form"):
        input_date = st.date_input("日付", value=date.today())
        company_name = st.text_input("会社名", value=st.session_state.get("sales_company", ""))
        staff = st.text_input("担当者名", value=user_name)
        new_cases = st.number_input("新規件数", min_value=0, value=0)
        teleapo = st.number_input("テレアポ件数", min_value=0, value=0)
        visits = st.number_input("訪問件数", min_value=0, value=1 if st.session_state.get("sales_from_visit") else 0)
        contracts = st.number_input("成約件数", min_value=0, value=1 if st.session_state.get("sales_from_visit") else 0)
        result_code = st.selectbox("結果コード", [""] + RESULT_CODE_OPTIONS, index=([""] + RESULT_CODE_OPTIONS).index("A 成立") if st.session_state.get("sales_from_visit") else 0)
        insurance_type = st.selectbox(
            "保険種類",
            [""] + ALL_INSURANCE_OPTIONS,
            index=([""] + ALL_INSURANCE_OPTIONS).index(st.session_state.get("sales_insurance_type", "")) if st.session_state.get("sales_insurance_type", "") in [""] + ALL_INSURANCE_OPTIONS else 0
        )
        carrier_type = st.selectbox(
            "AIG / 他社",
            ["AIG", "他社"],
            index=0 if st.session_state.get("sales_carrier_type", "AIG") == "AIG" else 1
        )
        renewal_month = st.selectbox(
            "更新月",
            list(range(0, 13)),
            index=safe_int(st.session_state.get("sales_renewal_month", 0)),
            format_func=lambda x: "未設定" if x == 0 else f"{x}月"
        )
        sonpo_premium = st.number_input("新規損保S（円）", min_value=0, value=0, step=10000)
        sonpo_commission = st.number_input("損保手数料（円）", min_value=0, value=0, step=1000)
        seiho_new_s = st.number_input("新規生保S（円）", min_value=0, value=0, step=10000)
        seiho_commission = st.number_input("生保手数料（円）", min_value=0, value=0, step=1000)
        inforce_premium = st.number_input("保有収保（円）", min_value=0, value=0, step=10000)
        memo = st.text_input("備考", value="")
        submitted = st.form_submit_button("保存")

        if submitted:
            insert_kpi((
                str(input_date), company_name, staff, new_cases, teleapo, visits, contracts,
                sonpo_premium, sonpo_commission, seiho_new_s, seiho_commission,
                inforce_premium, result_code, insurance_type, carrier_type,
                renewal_month, memo
            ))
            st.success("保存しました。")
            st.session_state["sales_from_visit"] = False
            st.session_state["sales_company"] = ""
            st.session_state["sales_insurance_type"] = ""
            st.session_state["sales_carrier_type"] = "AIG"
            st.session_state["sales_renewal_month"] = 0
            st.rerun()

elif menu == "訪問履歴":
    st.subheader("訪問履歴")
    tab1, tab2 = st.tabs(["履歴追加", "履歴一覧"])

    with tab1:
        if not customer_names:
            st.info("先に顧客を登録してください。")
        else:
            activity_customer = st.selectbox("顧客", customer_names)
            customer_row = customers_df[customers_df["company_name"] == activity_customer].iloc[0]
            quick_type = st.radio("活動種別", ACTIVITY_TYPE_OPTIONS, horizontal=True)
            activity_date = st.date_input("日付", value=date.today())
            activity_staff = st.text_input("担当者", value=user_name)
            activity_insurance = st.selectbox("提案商品", [""] + ALL_INSURANCE_OPTIONS)
            activity_memo = st.text_input("内容メモ")
            activity_next_action = st.text_input("次回アクション")
            activity_next_date = st.date_input("次回予定日", value=date.today())
            activity_result = st.radio("成果", RESULT_OPTIONS, horizontal=True)
            activity_temp = st.radio("温度感", TEMPERATURE_OPTIONS, horizontal=True)

            if st.button("履歴を保存", use_container_width=True):
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
            st.dataframe(activity_df, use_container_width=True)

elif menu == "案件管理":
    st.subheader("案件管理")
    tab1, tab2 = st.tabs(["案件追加", "案件一覧"])

    with tab1:
        if customer_names:
            opp_customer = st.selectbox("顧客", customer_names)
            customer_row = customers_df[customers_df["company_name"] == opp_customer].iloc[0]
            opp_insurance = st.selectbox("保険種類", ALL_INSURANCE_OPTIONS)
            opp_status = st.radio("ステータス", OPPORTUNITY_STATUS_OPTIONS, horizontal=True)
            opp_premium = st.number_input("見込保険料（円）", min_value=0, value=0, step=10000)
            opp_commission = st.number_input("見込手数料（円）", min_value=0, value=0, step=1000)
            opp_renewal_month = st.selectbox("更新月", list(range(0, 13)), format_func=lambda x: "未設定" if x == 0 else f"{x}月")
            opp_probability = st.slider("確度（%）", 0, 100, 50)
            opp_memo = st.text_input("備考")

            if st.button("案件を保存", use_container_width=True):
                insert_opportunity((
                    int(customer_row["customer_id"]), opp_customer, opp_insurance,
                    opp_status, opp_premium, opp_commission, opp_renewal_month,
                    opp_probability, opp_memo
                ))
                st.success("案件を保存しました。")
                st.rerun()

    with tab2:
        if opportunities_df.empty:
            st.info("案件なし")
        else:
            st.dataframe(opportunities_df, use_container_width=True)

elif menu == "DM発送履歴":
    st.subheader("DM発送履歴")
    tab1, tab2, tab3 = st.tabs(["DM追加", "DM一覧", "未接触アラート"])

    with tab1:
        if customer_names:
            dm_customer = st.selectbox("顧客", customer_names)
            customer_row = customers_df[customers_df["company_name"] == dm_customer].iloc[0]
            dm_send_date = st.date_input("発送日", value=date.today())
            dm_type = st.radio("DM種類", DM_TYPE_OPTIONS, horizontal=True)
            dm_title = st.text_input("タイトル")
            dm_memo = st.text_input("メモ")
            dm_staff = st.text_input("担当者", value=user_name)
            dm_followup_due_date = st.date_input("フォロー予定日", value=date.today() + timedelta(days=14))
            dm_followup_done = st.selectbox("フォロー対応", ["未対応", "済"])
            dm_reaction = st.selectbox("反応", DM_REACTION_OPTIONS)

            if st.button("DM履歴を保存", use_container_width=True):
                insert_dm((
                    str(dm_send_date), int(customer_row["customer_id"]), dm_customer,
                    dm_type, dm_title, dm_memo, dm_staff,
                    str(dm_followup_due_date), dm_followup_done, dm_reaction
                ))
                st.success("DM履歴を保存しました。")
                st.rerun()

    with tab2:
        if dm_history_df.empty:
            st.info("DM履歴なし")
        else:
            st.dataframe(dm_history_df, use_container_width=True)

    with tab3:
        alerts = build_dm_alerts(dm_history_df, activity_df)
        if alerts.empty:
            st.info("未接触アラートはありません。")
        else:
            st.dataframe(alerts, use_container_width=True)

elif menu == "保険加入状況":
    st.subheader("保険加入状況")
    tab1, tab2 = st.tabs(["加入状況追加", "加入状況一覧"])

    with tab1:
        if customer_names:
            status_customer = st.selectbox("顧客", customer_names)
            customer_row = customers_df[customers_df["company_name"] == status_customer].iloc[0]
            status_insurance = st.selectbox("保険種類", ALL_INSURANCE_OPTIONS)
            status_progress = st.radio("進捗", INSURANCE_PROGRESS_OPTIONS, horizontal=True)
            status_memo = st.text_input("備考")
            if st.button("加入状況を保存", use_container_width=True):
                insert_insurance_status((
                    int(customer_row["customer_id"]), status_customer, status_insurance, status_progress, status_memo
                ))
                st.success("加入状況を保存しました。")
                st.rerun()

    with tab2:
        if insurance_status_df.empty:
            st.info("データなし")
        else:
            st.dataframe(insurance_status_df, use_container_width=True)

elif menu == "宛名印刷":
    st.subheader("宛名印刷")
    if customers_df.empty:
        st.info("顧客データがありません。")
    else:
        mode = st.radio("印刷形式", ["A4ラベル", "ハガキ"], horizontal=True)
        count = st.number_input("表示件数", min_value=1, max_value=max(1, len(customers_df)), value=min(12, max(1, len(customers_df))))
        preview_df = customers_df.head(count).copy()

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

elif menu == "ダッシュボード":
    st.subheader("ダッシュボード")
    monthly = to_monthly(kpi_df)

    if monthly.empty:
        st.info("まだデータがありません。")
    else:
        latest_month = monthly["month"].iloc[-1]
        current = monthly[monthly["month"] == latest_month].iloc[0]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("今月新規件数", f"{safe_int(current['new_cases'])}件")
        c2.metric("今月テレアポ", f"{safe_int(current['teleapo'])}件")
        c3.metric("今月訪問", f"{safe_int(current['visits'])}件")
        c4.metric("今月成約", f"{safe_int(current['contracts'])}件")
        c5.metric("保有収保", format_currency(current["inforce_premium"]))

        c6, c7, c8, c9 = st.columns(4)
        c6.metric("新規損保S", format_currency(current["sonpo_premium"]))
        c7.metric("損保手数料", format_currency(current["sonpo_commission"]))
        c8.metric("新規生保S", format_currency(current["seiho_new_s"]))
        c9.metric("生保手数料", format_currency(current["seiho_commission"]))

        st.dataframe(monthly, use_container_width=True)

elif menu == "月次集計":
    st.subheader("月次集計")
    monthly = to_monthly(kpi_df)
    if monthly.empty:
        st.info("まだデータがありません。")
    else:
        st.dataframe(monthly, use_container_width=True)

elif menu == "目標設定":
    st.subheader("目標設定")

    with st.form("targets_form"):
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

    c1, c2 = st.columns(2)
    with c1:
        current_inforce = st.number_input("現在の保有収保（円）", min_value=0, value=latest_inforce, step=10000)
        monthly_new_cases = st.number_input("毎月の新規件数", min_value=0, value=2)
        avg_premium_per_case = st.number_input("1件あたり平均保険料（円）", min_value=0, value=avg_premium_per_case_default, step=10000)
    with c2:
        avg_commission_per_case = st.number_input("1件あたり平均損保手数料（円）", min_value=0, value=avg_commission_per_case_default, step=1000)
        annual_retention_rate = st.number_input("継続率（%）", min_value=0.0, max_value=100.0, value=90.0, step=0.5)

    forecast_df = make_forecast(
        current_inforce,
        monthly_new_cases,
        avg_premium_per_case,
        avg_commission_per_case,
        annual_retention_rate,
    )
    st.dataframe(forecast_df, use_container_width=True)
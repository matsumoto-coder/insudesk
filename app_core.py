from datetime import date, datetime, timedelta
from pathlib import Path
import shutil
import sqlite3

import pandas as pd
import requests

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

INDUSTRY_OPTIONS = [
    "A 農業・林業",
    "B 漁業",
    "C 鉱業・採石業・砂利採取業",
    "D 建設業",
    "E 製造業",
    "F 電気・ガス・熱供給・水道業",
    "G 情報通信業",
    "H 運輸業・郵便業",
    "I 卸売業・小売業",
    "J 金融業・保険業",
    "K 不動産業・物品賃貸業",
    "L 学術研究・専門技術サービス業",
    "M 宿泊業・飲食サービス業",
    "N 生活関連サービス業・娯楽業",
    "O 教育・学習支援業",
    "P 医療・福祉",
    "Q 複合サービス事業",
    "R サービス業（他に分類されないもの）",
    "S 公務（他に分類されるものを除く）",
    "T 分類不能の産業",
]

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
POLICY_STATUS_OPTIONS = ["加入中", "解約"]
CARRIER_OPTIONS = ["AIG", "他社"]


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def exec_sql(sql: str, params=(), fetch: bool = False):
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
    return df.fillna("")


def safe_int(value):
    try:
        if pd.isna(value) or value == "":
            return 0
        return int(float(value))
    except Exception:
        return 0


def safe_float(value):
    try:
        if pd.isna(value) or value == "":
            return 0.0
        return float(value)
    except Exception:
        return 0.0


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


def normalize_renewal_month(value):
    if value in ["", None, 0, "0"]:
        return 0
    return safe_int(value)


def renewal_label(month_value):
    month_value = normalize_renewal_month(month_value)
    return "空欄" if month_value == 0 else f"{month_value}月"


def get_address_from_zip(postal_code: str) -> str:
    zipcode = "".join(ch for ch in str(postal_code) if ch.isdigit())
    if len(zipcode) != 7:
        return ""
    try:
        response = requests.get(
            "https://zipcloud.ibsnet.co.jp/api/search",
            params={"zipcode": zipcode},
            timeout=8,
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("results") or []
        if not results:
            return ""
        row = results[0]
        return f"{row.get('address1', '')}{row.get('address2', '')}{row.get('address3', '')}".strip()
    except Exception:
        return ""


# =========================================================
# 追加プログラムここから：BCP診断
# =========================================================
def run_bcp_assessment(customer_row):
    company_name = str(customer_row.get("company_name", "")).strip()
    address1 = str(customer_row.get("address1", "")).strip()
    industry = str(customer_row.get("industry", "")).strip()
    employee_count = safe_int(customer_row.get("employee_count", 0))
    nankai_priority = str(customer_row.get("nankai_priority", "")).strip()
    bcp_exists = str(customer_row.get("bcp_exists", "")).strip()
    continuity_plan_applied = str(customer_row.get("continuity_plan_applied", "")).strip()

    score = 0
    reasons = []
    recommendations = []

    if address1:
        score += 10
        reasons.append("住所情報あり")
        recommendations.append("所在地ハザードマップの確認")
    else:
        reasons.append("住所未登録")
        recommendations.append("所在地情報の登録")

    high_risk_industries = [
        "A 農業・林業",
        "D 建設業",
        "E 製造業",
        "H 運輸業・郵便業",
        "P 医療・福祉",
    ]
    medium_risk_industries = [
        "B 漁業",
        "C 鉱業・採石業・砂利採取業",
        "F 電気・ガス・熱供給・水道業",
        "I 卸売業・小売業",
        "K 不動産業・物品賃貸業",
        "M 宿泊業・飲食サービス業",
        "N 生活関連サービス業・娯楽業",
        "Q 複合サービス事業",
        "R サービス業（他に分類されないもの）",
    ]
    low_risk_industries = [
        "G 情報通信業",
        "J 金融業・保険業",
        "L 学術研究・専門技術サービス業",
        "O 教育・学習支援業",
        "S 公務（他に分類されるものを除く）",
        "T 分類不能の産業",
    ]

    if industry in high_risk_industries:
        score += 25
        reasons.append(f"業種リスク高め（{industry}）")
    elif industry in medium_risk_industries:
        score += 15
        reasons.append(f"業種登録あり（{industry}）")
    elif industry in low_risk_industries:
        score += 10
        reasons.append(f"業種登録あり（{industry}）")
    elif industry:
        score += 10
        reasons.append(f"業種登録あり（{industry}）")
    else:
        reasons.append("業種未登録")

    if employee_count >= 50:
        score += 20
        reasons.append("従業員50名以上")
    elif employee_count >= 10:
        score += 10
        reasons.append("従業員10名以上")

    if nankai_priority == "高":
        score += 30
        reasons.append("南海トラフ優先度：高")
    elif nankai_priority == "中":
        score += 15
        reasons.append("南海トラフ優先度：中")
    elif nankai_priority == "低":
        score += 5
        reasons.append("南海トラフ優先度：低")

    if bcp_exists == "無":
        score += 20
        reasons.append("BCP未策定")
        recommendations.append("BCP初期整備")
    elif bcp_exists == "有":
        reasons.append("BCP策定済")

    if continuity_plan_applied == "無":
        score += 10
        reasons.append("事業継続強化計画 未申請")
        recommendations.append("事業継続強化計画の検討")
    elif continuity_plan_applied == "有":
        reasons.append("事業継続強化計画 申請済")

    base_recommendations = [
        "連絡網の整備",
        "顧客データのバックアップ",
        "代替連絡手段の確認",
        "初動資金の確保",
    ]
    for rec in base_recommendations:
        if rec not in recommendations:
            recommendations.append(rec)

    if score >= 70:
        level = "BCP優先対応"
        priority_color = "🔴"
    elif score >= 40:
        level = "BCP対応推奨"
        priority_color = "🟠"
    else:
        level = "BCP基礎確認"
        priority_color = "🟢"

    return {
        "company_name": company_name,
        "score": score,
        "level": level,
        "priority_color": priority_color,
        "reasons": reasons,
        "recommendations": recommendations,
    }
# =========================================================
# 追加プログラムここまで：BCP診断
# =========================================================


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_policies (
            policy_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            customer_name TEXT,
            insurance_type TEXT,
            carrier_type TEXT,
            status TEXT,
            renewal_month INTEGER DEFAULT 0,
            memo TEXT,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS insurance_status (
            status_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            customer_name TEXT,
            insurance_type TEXT,
            progress TEXT,
            memo TEXT
        )
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS todos (
            todo_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            task_name TEXT,
            due_date TEXT,
            status TEXT,
            memo TEXT,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        INSERT OR IGNORE INTO targets (
            id, new_cases_target, teleapo_target, visits_target, contracts_target,
            sonpo_premium_target, sonpo_commission_target, seiho_new_s_target,
            seiho_commission_target, inforce_premium_target
        ) VALUES (1, 5, 100, 30, 6, 3000000, 50000, 1000000, 800000, 30000000)
        """
    )

    conn.commit()
    conn.close()


def get_customers_df():
    return load_df("SELECT * FROM customers ORDER BY customer_id DESC")


def get_customer_policies_df():
    return load_df("SELECT * FROM customer_policies ORDER BY policy_id DESC")


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


def insert_customer(data):
    exec_sql(
        """
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
        """,
        data,
    )


def update_customer(customer_id, data):
    exec_sql(
        """
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
        """,
        data + (customer_id,),
    )


def delete_customer(customer_id):
    exec_sql("DELETE FROM customers WHERE customer_id=?", (customer_id,))
    exec_sql("DELETE FROM customer_policies WHERE customer_id=?", (customer_id,))
    exec_sql("DELETE FROM activity_log WHERE customer_id=?", (customer_id,))
    exec_sql("DELETE FROM opportunities WHERE customer_id=?", (customer_id,))
    exec_sql("DELETE FROM insurance_status WHERE customer_id=?", (customer_id,))
    exec_sql("DELETE FROM dm_history WHERE customer_id=?", (customer_id,))
    exec_sql("DELETE FROM visit_logs WHERE customer_id=?", (customer_id,))


def insert_policy(data):
    exec_sql(
        """
        INSERT INTO customer_policies (
            customer_id, customer_name, insurance_type, carrier_type,
            status, renewal_month, memo, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )


def update_policy(policy_id, data):
    exec_sql(
        """
        UPDATE customer_policies SET
            insurance_type=?, carrier_type=?, status=?, renewal_month=?, memo=?
        WHERE policy_id=?
        """,
        data + (policy_id,),
    )


def delete_policy(policy_id):
    exec_sql("DELETE FROM customer_policies WHERE policy_id=?", (policy_id,))


def insert_kpi(data):
    exec_sql(
        """
        INSERT INTO kpi_data (
            date, company_name, staff, new_cases, teleapo, visits, contracts,
            sonpo_premium, sonpo_commission, seiho_new_s, seiho_commission,
            inforce_premium, result_code, insurance_type, carrier_type,
            renewal_month, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )


def update_targets(data):
    exec_sql(
        """
        UPDATE targets SET
            new_cases_target=?, teleapo_target=?, visits_target=?, contracts_target=?,
            sonpo_premium_target=?, sonpo_commission_target=?, seiho_new_s_target=?,
            seiho_commission_target=?, inforce_premium_target=?
        WHERE id=1
        """,
        data,
    )


def insert_activity(data):
    exec_sql(
        """
        INSERT INTO activity_log (
            date, customer_id, customer_name, activity_type, staff,
            insurance_type, memo, next_action, next_date, result, temperature
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )


def insert_opportunity(data):
    exec_sql(
        """
        INSERT INTO opportunities (
            customer_id, customer_name, insurance_type, status,
            estimated_premium, estimated_commission, renewal_month, probability, memo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )


def insert_insurance_status(data):
    exec_sql(
        """
        INSERT INTO insurance_status (
            customer_id, customer_name, insurance_type, progress, memo
        ) VALUES (?, ?, ?, ?, ?)
        """,
        data,
    )


def insert_dm(data):
    exec_sql(
        """
        INSERT INTO dm_history (
            send_date, customer_id, customer_name, dm_type, title,
            memo, staff, followup_due_date, followup_done, reaction
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )


def insert_todo(data):
    exec_sql(
        """
        INSERT INTO todos (
            company_name, task_name, due_date, status, memo, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        data,
    )


def complete_todo(todo_id):
    exec_sql("UPDATE todos SET status='完了' WHERE todo_id=?", (todo_id,))


def insert_visit_start(customer_id, company_name):
    exec_sql(
        """
        INSERT INTO visit_logs (
            customer_id, company_name, visit_start, visit_end,
            duration_minutes, result_code, memo, insurance_type,
            carrier_type, renewal_month, created_at
        ) VALUES (?, ?, ?, NULL, 0, '', '', '', '', 0, ?)
        """,
        (
            customer_id,
            company_name,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def get_open_visit(customer_id):
    rows = exec_sql(
        """
        SELECT * FROM visit_logs
        WHERE customer_id=? AND (visit_end IS NULL OR visit_end='')
        ORDER BY visit_id DESC LIMIT 1
        """,
        (customer_id,),
        fetch=True,
    )
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

    exec_sql(
        """
        UPDATE visit_logs
        SET visit_end=?, duration_minutes=?, result_code=?, memo=?,
            insurance_type=?, carrier_type=?, renewal_month=?
        WHERE visit_id=?
        """,
        (
            end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            duration,
            result_code,
            memo,
            insurance_type,
            carrier_type,
            normalize_renewal_month(renewal_month),
            visit_id,
        ),
    )
    return {
        "visit_id": visit_id,
        "duration_minutes": duration,
        "start": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "end": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }


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


def build_calendar_events(customers_df, activity_df, dm_df, todos_df, policies_df):
    events = []

    if not customers_df.empty:
        for _, row in customers_df.iterrows():
            next_d = parse_date_str(row.get("next_action_date", ""))
            if next_d and str(row.get("next_action", "")).strip():
                events.append({"date": next_d, "type": "todo", "label": f"{row.get('company_name','')}｜{row.get('next_action','')}"})

    if not policies_df.empty:
        for _, row in policies_df.iterrows():
            renewal_month = normalize_renewal_month(row.get("renewal_month", 0))
            if renewal_month > 0 and str(row.get("status", "加入中")) == "加入中":
                renewal_date = date(date.today().year, renewal_month, 1)
                events.append({
                    "date": renewal_date,
                    "type": "renewal",
                    "label": f"{row.get('customer_name','')}｜{row.get('insurance_type','')} 更改月",
                })

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
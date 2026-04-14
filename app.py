import streamlit as st

from app_core import (
    init_db,
    get_customers_df,
    get_customer_policies_df,
    get_kpi_df,
    get_activity_df,
    get_opportunities_df,
    get_insurance_status_df,
    get_dm_history_df,
    get_todos_df,
    get_visit_logs_df,
)

from app_features import (
    show_calendar_page,
    show_todo_page,
    show_customer_page,
    show_customer_detail_page,
    show_daily_input_page,
    show_visit_page,
    show_opportunity_page,
    show_dm_page,
    show_insurance_status_page,
    show_address_print_page,
    show_dashboard_page,
    show_monthly_page,
    show_target_page,
    show_forecast_page,
    show_backup_page,
    show_mobile_home_page,
)

APP_TITLE = "InsuDesk"

st.set_page_config(
    page_title=APP_TITLE,
    layout="wide"
)

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

# =====================================================
# 初期化
# =====================================================
init_db()

customers_df = get_customers_df()
policies_df = get_customer_policies_df()
kpi_df = get_kpi_df()
activity_df = get_activity_df()
opportunities_df = get_opportunities_df()
insurance_status_df = get_insurance_status_df()
dm_history_df = get_dm_history_df()
todos_df = get_todos_df()
visit_logs_df = get_visit_logs_df()

customer_names = (
    customers_df["company_name"].dropna().astype(str).tolist()
    if not customers_df.empty
    else []
)

if "menu" not in st.session_state:
    st.session_state["menu"] = "カレンダー"

if "sales_from_visit" not in st.session_state:
    st.session_state["sales_from_visit"] = False

user_name = "本人"

# =====================================================
# サイドバー
# =====================================================
st.sidebar.title(APP_TITLE)
st.sidebar.caption(f"ログイン中: {user_name}")

menu_groups = [
    ["カレンダー", "ToDo一覧"],
    ["顧客管理", "顧客詳細"],
    ["日次入力", "訪問履歴", "案件管理", "DM発送履歴"],
    ["保険加入状況", "宛名印刷"],
    ["ダッシュボード", "月次集計", "目標設定", "5年予測"],
    ["バックアップ", "スマホ簡易ホーム"],
]

for group in menu_groups:
    for item in group:
        if st.sidebar.button(item, use_container_width=True, key=f"menu_{item}"):
            st.session_state["menu"] = item
    st.sidebar.markdown("---")

menu = st.session_state["menu"]

st.title(APP_TITLE)
st.caption(f"現在メニュー: {menu}")

# =====================================================
# ページルーティング
# =====================================================
if menu == "カレンダー":
    show_calendar_page(
        customers_df,
        activity_df,
        dm_history_df,
        todos_df,
        policies_df,
    )

elif menu == "ToDo一覧":
    show_todo_page(
        customers_df,
        activity_df,
        dm_history_df,
        todos_df,
    )

elif menu == "顧客管理":
    show_customer_page(customers_df, user_name)

elif menu == "顧客詳細":
    show_customer_detail_page(
        customers_df,
        policies_df,
        visit_logs_df,
        customer_names,
    )

elif menu == "日次入力":
    show_daily_input_page(kpi_df, user_name)

elif menu == "訪問履歴":
    show_visit_page(customers_df, customer_names, user_name)

elif menu == "案件管理":
    show_opportunity_page(
        customers_df,
        opportunities_df,
        customer_names,
    )

elif menu == "DM発送履歴":
    show_dm_page(
        customers_df,
        activity_df,
        dm_history_df,
        customer_names,
        user_name,
    )

elif menu == "保険加入状況":
    show_insurance_status_page(
        customers_df,
        insurance_status_df,
        customer_names,
    )

elif menu == "宛名印刷":
    show_address_print_page(customers_df)

elif menu == "ダッシュボード":
    show_dashboard_page(kpi_df, visit_logs_df)

elif menu == "月次集計":
    show_monthly_page(kpi_df)

elif menu == "目標設定":
    show_target_page()

elif menu == "5年予測":
    show_forecast_page(kpi_df)

elif menu == "バックアップ":
    show_backup_page()

elif menu == "スマホ簡易ホーム":
    show_mobile_home_page(
        customers_df,
        activity_df,
        dm_history_df,
        todos_df,
    )
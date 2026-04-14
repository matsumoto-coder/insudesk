from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st

from app_core import (
    ALL_INSURANCE_OPTIONS,
    ACTIVITY_TYPE_OPTIONS,
    BACKUP_DIR,
    CARRIER_OPTIONS,
    DB_PATH,
    DM_REACTION_OPTIONS,
    DM_TYPE_OPTIONS,
    IMAGE_DIR,
    INDUSTRY_OPTIONS,
    INSURANCE_PROGRESS_OPTIONS,
    KEISHO_OPTIONS,
    NANKAI_PRIORITY_OPTIONS,
    OPPORTUNITY_STATUS_OPTIONS,
    POLICY_STATUS_OPTIONS,
    RESULT_CODE_OPTIONS,
    RESULT_OPTIONS,
    TEMPERATURE_OPTIONS,
    backup_to_local,
    build_calendar_events,
    build_dm_alerts,
    build_todo_df,
    complete_todo,
    days_since,
    delete_customer,
    delete_policy,
    finish_visit,
    format_currency,
    get_address_from_zip,
    get_conn,
    insert_activity,
    insert_customer,
    insert_dm,
    insert_insurance_status,
    insert_kpi,
    insert_opportunity,
    insert_policy,
    insert_todo,
    insert_visit_start,
    list_local_backups,
    make_forecast,
    month_calendar_dates,
    normalize_renewal_month,
    parse_date_str,
    renewal_label,
    safe_int,
    to_monthly,
    update_customer,
    update_policy,
    update_targets,
)
from bcp.ui import render_bcp_section


def show_calendar_page(customers_df, activity_df, dm_history_df, todos_df, policies_df):
    st.subheader("カレンダー")

    events = build_calendar_events(customers_df, activity_df, dm_history_df, todos_df, policies_df)
    view_mode = st.radio("表示切替", ["月表示", "週表示", "日表示"], horizontal=True)
    base_date = st.date_input("基準日", value=date.today(), key="calendar_base_date")

    event_map = {}
    for event in events:
        event_map.setdefault(event["date"], []).append(event)

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
                    st.markdown(
                        f'<div class="section-box"><div style="{muted}"><b>{d.day}</b></div>',
                        unsafe_allow_html=True,
                    )
                    for event in event_map.get(d, [])[:4]:
                        st.write(f"• {event['label']}")
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
                    for event in day_events:
                        st.write(f"• {event['label']}")
                st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.write(f"### {base_date.strftime('%Y-%m-%d')} の予定")
        day_events = event_map.get(base_date, [])
        if not day_events:
            st.info("予定はありません。")
        else:
            st.dataframe(pd.DataFrame(day_events), use_container_width=True)


def show_todo_page(customers_df, activity_df, dm_history_df, todos_df):
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
        due_date = st.date_input("期限", value=date.today() + timedelta(days=7), key="manual_todo_due")
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
            format_func=lambda x: f"{todos_df[todos_df['todo_id'] == x].iloc[0]['company_name']}｜{todos_df[todos_df['todo_id'] == x].iloc[0]['task_name']}",
        )
        if st.button("選択ToDoを完了"):
            complete_todo(int(target_todo))
            st.success("完了にしました。")
            st.rerun()


def show_customer_page(customers_df, user_name):
    st.subheader("顧客管理")
    tab1, tab2, tab3 = st.tabs(["顧客追加", "顧客編集", "顧客一覧"])

    with tab1:
        business_card_file = st.file_uploader(
            "名刺画像アップロード",
            type=["png", "jpg", "jpeg", "webp"],
            key="business_card_upload",
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

        c_zip1, c_zip2 = st.columns([1, 1])
        with c_zip1:
            if st.button("郵便番号から住所入力", use_container_width=True):
                auto_address = get_address_from_zip(postal_code)
                st.session_state["zip_auto_address"] = auto_address
                if not auto_address:
                    st.warning("住所を取得できませんでした。郵便番号を確認してください。")
        with c_zip2:
            if st.session_state.get("zip_auto_address", ""):
                st.success("住所候補を取得しました")

        address1 = st.text_input("住所1", value=st.session_state.get("zip_auto_address", ""))
        address2 = st.text_input("住所2")
        website_url = st.text_input("会社HP / Webサイト", placeholder="https://...")
        staff = st.text_input("自社担当者", value=user_name)
        status = st.selectbox("状態", ["見込", "提案中", "契約中", "失注", "既契約"])
        customer_rank = st.selectbox("顧客ランク", ["A", "B", "C"])
        insurance_types = st.multiselect("参考用 保険種類", ALL_INSURANCE_OPTIONS)
        sonpo_annual_premium = st.number_input("損保年間保険料（円）", min_value=0, value=0, step=10000)
        seiho_annual_premium = st.number_input("生保年間保険料（円）", min_value=0, value=0, step=10000)
        renewal_month = st.selectbox("参考用 更新月", list(range(0, 13)), format_func=renewal_label)

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
            placeholder="面談メモや OneNote URL を貼り付け",
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
                    category,
                    company_name,
                    department_name,
                    attn_name,
                    keisho,
                    contact_name,
                    rep_phone,
                    contact_phone,
                    phone,
                    email,
                    postal_code,
                    address1,
                    address2,
                    website_url,
                    staff,
                    status,
                    customer_rank,
                    ",".join(insurance_types),
                    industry,
                    annual_sales,
                    employee_count,
                    nankai_priority,
                    bcp_exists,
                    continuity_plan_applied,
                    sonpo_annual_premium,
                    seiho_annual_premium,
                    normalize_renewal_month(renewal_month),
                    memo,
                    business_card_path,
                    str(last_contact_date),
                    next_action,
                    str(next_action_date),
                ))
                st.session_state["zip_auto_address"] = ""
                st.success("顧客を保存しました。")
                st.rerun()

    with tab2:
        if customers_df.empty:
            st.info("顧客データがありません。")
        else:
            edit_target = st.selectbox("編集する顧客を選択", customers_df["company_name"].tolist())
            row = customers_df[customers_df["company_name"] == edit_target].iloc[0]

            edit_category = st.radio("区分", ["法人", "個人"], horizontal=True, index=0 if row["category"] == "法人" else 1)
            edit_company_name = st.text_input("法人名 / 個人名", value=row["company_name"], key="edit_company_name")
            edit_department_name = st.text_input("部署名", value=row["department_name"])
            edit_attn_name = st.text_input("宛名名", value=row["attn_name"])
            edit_keisho = st.selectbox("敬称", KEISHO_OPTIONS, index=KEISHO_OPTIONS.index(row["keisho"]) if row["keisho"] in KEISHO_OPTIONS else 0)
            edit_contact_name = st.text_input("担当者氏名", value=row["contact_name"])
            edit_rep_phone = st.text_input("法人代表電話", value=row["rep_phone"])
            edit_contact_phone = st.text_input("担当者電話番号", value=row["contact_phone"])
            edit_phone = st.text_input("その他電話番号", value=row["phone"])
            edit_email = st.text_input("メール", value=row["email"])
            edit_postal_code = st.text_input("郵便番号", value=row["postal_code"])
            edit_address1 = st.text_input("住所1", value=row["address1"])
            edit_address2 = st.text_input("住所2", value=row["address2"])
            edit_website_url = st.text_input("会社HP / Webサイト", value=row["website_url"])
            edit_staff = st.text_input("自社担当者", value=row["staff"])
            edit_status = st.selectbox(
                "状態",
                ["見込", "提案中", "契約中", "失注", "既契約"],
                index=["見込", "提案中", "契約中", "失注", "既契約"].index(row["status"]) if row["status"] in ["見込", "提案中", "契約中", "失注", "既契約"] else 0,
            )
            edit_customer_rank = st.selectbox(
                "顧客ランク",
                ["A", "B", "C"],
                index=["A", "B", "C"].index(row["customer_rank"]) if row["customer_rank"] in ["A", "B", "C"] else 0,
            )
            current_ins_types = [x for x in str(row["insurance_types"]).split(",") if x]
            edit_insurance_types = st.multiselect("参考用 保険種類", ALL_INSURANCE_OPTIONS, default=current_ins_types)
            edit_sonpo_annual_premium = st.number_input("損保年間保険料（円）", min_value=0, value=safe_int(row["sonpo_annual_premium"]), step=10000, key="edit_sonpo")
            edit_seiho_annual_premium = st.number_input("生保年間保険料（円）", min_value=0, value=safe_int(row["seiho_annual_premium"]), step=10000, key="edit_seiho")
            edit_renewal_month = st.selectbox("参考用 更新月", list(range(0, 13)), index=normalize_renewal_month(row["renewal_month"]), format_func=renewal_label)

            if edit_category == "法人":
                edit_industry = st.selectbox(
                    "業種",
                    INDUSTRY_OPTIONS,
                    index=INDUSTRY_OPTIONS.index(row["industry"]) if row["industry"] in INDUSTRY_OPTIONS else 0,
                )
                edit_annual_sales = st.number_input("年商（円）", min_value=0, value=safe_int(row["annual_sales"]), step=1000000)
                edit_employee_count = st.number_input("従業員数", min_value=0, value=safe_int(row["employee_count"]))
                edit_nankai_priority = st.selectbox(
                    "南海トラフ優先度",
                    NANKAI_PRIORITY_OPTIONS,
                    index=NANKAI_PRIORITY_OPTIONS.index(row["nankai_priority"]) if row["nankai_priority"] in NANKAI_PRIORITY_OPTIONS else 0,
                )
                edit_bcp_exists = st.selectbox(
                    "BCP策定有無",
                    ["有", "無"],
                    index=["有", "無"].index(row["bcp_exists"]) if row["bcp_exists"] in ["有", "無"] else 0,
                )
                edit_continuity_plan_applied = st.selectbox(
                    "事業継続強化計画申請有無",
                    ["有", "無"],
                    index=["有", "無"].index(row["continuity_plan_applied"]) if row["continuity_plan_applied"] in ["有", "無"] else 0,
                )
            else:
                edit_industry = ""
                edit_annual_sales = 0
                edit_employee_count = 0
                edit_nankai_priority = ""
                edit_bcp_exists = ""
                edit_continuity_plan_applied = ""

            edit_memo = st.text_area("備考 / OneNoteリンク", value=row["memo"], height=100)
            current_card_path = str(row.get("business_card_image", "")).strip()
            if current_card_path and Path(current_card_path).exists():
                st.image(current_card_path, caption="現在の名刺画像", width=240)

            edit_business_card_file = st.file_uploader(
                "名刺画像を差し替え",
                type=["png", "jpg", "jpeg", "webp"],
                key="edit_business_card_upload",
            )

            edit_last_contact_date = st.date_input("最終接触日", value=parse_date_str(row["last_contact_date"]) or date.today())
            edit_next_action = st.text_input("次回アクション", value=row["next_action"])
            edit_next_action_date = st.date_input("次回予定日", value=parse_date_str(row["next_action_date"]) or date.today())

            c_edit1, c_edit2 = st.columns(2)
            with c_edit1:
                if st.button("顧客情報を更新", use_container_width=True):
                    business_card_path = current_card_path
                    if edit_business_card_file is not None:
                        ext = Path(edit_business_card_file.name).suffix.lower()
                        safe_name = edit_company_name.strip().replace(" ", "_").replace("/", "_")
                        image_path = IMAGE_DIR / f"{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
                        with open(image_path, "wb") as f:
                            f.write(edit_business_card_file.getbuffer())
                        business_card_path = str(image_path)

                    update_customer(
                        int(row["customer_id"]),
                        (
                            edit_category,
                            edit_company_name,
                            edit_department_name,
                            edit_attn_name,
                            edit_keisho,
                            edit_contact_name,
                            edit_rep_phone,
                            edit_contact_phone,
                            edit_phone,
                            edit_email,
                            edit_postal_code,
                            edit_address1,
                            edit_address2,
                            edit_website_url,
                            edit_staff,
                            edit_status,
                            edit_customer_rank,
                            ",".join(edit_insurance_types),
                            edit_industry,
                            edit_annual_sales,
                            edit_employee_count,
                            edit_nankai_priority,
                            edit_bcp_exists,
                            edit_continuity_plan_applied,
                            edit_sonpo_annual_premium,
                            edit_seiho_annual_premium,
                            normalize_renewal_month(edit_renewal_month),
                            edit_memo,
                            business_card_path,
                            str(edit_last_contact_date),
                            edit_next_action,
                            str(edit_next_action_date),
                        ),
                    )
                    st.success("更新しました。")
                    st.rerun()

            with c_edit2:
                if st.button("この顧客を削除", use_container_width=True):
                    delete_customer(int(row["customer_id"]))
                    st.success("削除しました。")
                    st.rerun()

    with tab3:
        if customers_df.empty:
            st.info("まだ顧客データがありません。")
        else:
            view_df = customers_df.copy().fillna("")
            view_df["未接触日数"] = view_df["last_contact_date"].apply(days_since)
            view_df["名刺画像"] = view_df["business_card_image"].apply(lambda x: "あり" if str(x).strip() else "")
            view_df["参考用更新月"] = view_df["renewal_month"].apply(renewal_label)

            columns = [
                "company_name",
                "category",
                "contact_name",
                "rep_phone",
                "contact_phone",
                "postal_code",
                "address1",
                "参考用更新月",
                "website_url",
                "名刺画像",
                "未接触日数",
            ]
            st.dataframe(view_df[columns], use_container_width=True)


def show_customer_detail_page(customers_df, policies_df, visit_logs_df, customer_names):
    st.subheader("顧客詳細")

    if not customer_names:
        st.info("顧客データがありません。")
        return

    selected_customer = st.selectbox("顧客を選択", customer_names)
    row = customers_df[customers_df["company_name"] == selected_customer].iloc[0]
    customer_id = int(row["customer_id"])
    customer_policies = policies_df[policies_df["customer_id"] == customer_id].copy() if not policies_df.empty else pd.DataFrame()

    st.markdown('<div class="visit-box">', unsafe_allow_html=True)
    st.markdown(f"## {row['company_name']}")
    st.info(f"現在表示中: {row['company_name']}")

    result_code = st.radio("今回の商談結果", RESULT_CODE_OPTIONS, horizontal=True, key="visit_result_code")

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
        renewal_month_choice = st.session_state.get("visit_renewal_month", 0)

        finish_memo = st.text_input("終了メモ", value=finish_memo, key="visit_finish_memo")
        insurance_type_choice = st.selectbox(
            "保険種類",
            [""] + ALL_INSURANCE_OPTIONS,
            index=([""] + ALL_INSURANCE_OPTIONS).index(insurance_type_choice) if insurance_type_choice in [""] + ALL_INSURANCE_OPTIONS else 0,
            key="visit_insurance_type",
        )
        carrier_type_choice = st.selectbox(
            "AIG / 他社",
            CARRIER_OPTIONS,
            index=CARRIER_OPTIONS.index(carrier_type_choice) if carrier_type_choice in CARRIER_OPTIONS else 0,
            key="visit_carrier_type",
        )
        renewal_month_choice = st.selectbox(
            "更新月",
            list(range(0, 13)),
            index=normalize_renewal_month(renewal_month_choice),
            format_func=renewal_label,
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
                    st.session_state["sales_renewal_month"] = normalize_renewal_month(renewal_month_choice)
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

    st.markdown("</div>", unsafe_allow_html=True)

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

    st.write("### 加入中の保険種類")
    with st.form("add_policy"):
        policy_type = st.selectbox("保険種類", ALL_INSURANCE_OPTIONS)
        carrier_type = st.selectbox("AIG / 他社", CARRIER_OPTIONS)
        policy_status = st.selectbox("状態", POLICY_STATUS_OPTIONS)
        policy_renewal_month = st.selectbox("更新月", list(range(0, 13)), format_func=renewal_label)
        policy_memo = st.text_input("メモ")
        add_policy_submitted = st.form_submit_button("契約追加")
        if add_policy_submitted:
            insert_policy((
                customer_id,
                row["company_name"],
                policy_type,
                carrier_type,
                policy_status,
                normalize_renewal_month(policy_renewal_month),
                policy_memo,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            st.success("契約追加しました")
            st.rerun()

    if customer_policies.empty:
        st.info("契約はまだ登録されていません。")
    else:
        customer_policies = customer_policies.sort_values("policy_id", ascending=False)
        for _, p_row in customer_policies.iterrows():
            st.markdown('<div class="section-box">', unsafe_allow_html=True)
            st.write(
                f"**{p_row['insurance_type']}** / "
                f"{p_row['carrier_type']} / "
                f"{p_row['status']} / "
                f"{renewal_label(p_row['renewal_month'])}"
            )

            with st.form(f"edit_policy_{int(p_row['policy_id'])}"):
                ep1, ep2, ep3 = st.columns(3)
                with ep1:
                    edit_policy_type = st.selectbox(
                        "保険種類",
                        ALL_INSURANCE_OPTIONS,
                        index=ALL_INSURANCE_OPTIONS.index(p_row["insurance_type"]) if p_row["insurance_type"] in ALL_INSURANCE_OPTIONS else 0,
                        key=f"policy_type_{int(p_row['policy_id'])}",
                    )
                with ep2:
                    edit_policy_carrier = st.selectbox(
                        "AIG / 他社",
                        CARRIER_OPTIONS,
                        index=CARRIER_OPTIONS.index(p_row["carrier_type"]) if p_row["carrier_type"] in CARRIER_OPTIONS else 0,
                        key=f"policy_carrier_{int(p_row['policy_id'])}",
                    )
                with ep3:
                    edit_policy_status = st.selectbox(
                        "状態",
                        POLICY_STATUS_OPTIONS,
                        index=POLICY_STATUS_OPTIONS.index(p_row["status"]) if p_row["status"] in POLICY_STATUS_OPTIONS else 0,
                        key=f"policy_status_{int(p_row['policy_id'])}",
                    )

                eq1, eq2 = st.columns(2)
                with eq1:
                    edit_policy_renewal = st.selectbox(
                        "更新月",
                        list(range(0, 13)),
                        index=normalize_renewal_month(p_row["renewal_month"]),
                        format_func=renewal_label,
                        key=f"policy_renewal_{int(p_row['policy_id'])}",
                    )
                with eq2:
                    edit_policy_memo = st.text_input("メモ", value=p_row["memo"], key=f"policy_memo_{int(p_row['policy_id'])}")

                ebtn1, ebtn2 = st.columns(2)
                with ebtn1:
                    update_submitted = st.form_submit_button("更新")
                with ebtn2:
                    delete_submitted = st.form_submit_button("削除")

                if update_submitted:
                    update_policy(
                        int(p_row["policy_id"]),
                        (
                            edit_policy_type,
                            edit_policy_carrier,
                            edit_policy_status,
                            normalize_renewal_month(edit_policy_renewal),
                            edit_policy_memo,
                        ),
                    )
                    st.success("契約を更新しました。")
                    st.rerun()

                if delete_submitted:
                    delete_policy(int(p_row["policy_id"]))
                    st.success("契約を削除しました。")
                    st.rerun()

            st.markdown("</div>", unsafe_allow_html=True)

    render_bcp_section(row)

    st.write("### 訪問履歴")
    customer_visit_logs = visit_logs_df[visit_logs_df["customer_id"] == customer_id] if not visit_logs_df.empty else pd.DataFrame()
    if customer_visit_logs.empty:
        st.info("訪問履歴なし")
    else:
        display_visit_df = customer_visit_logs.copy()
        display_visit_df["renewal_month"] = display_visit_df["renewal_month"].apply(renewal_label)
        st.dataframe(display_visit_df, use_container_width=True)


def show_daily_input_page(kpi_df, user_name):
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
        result_code = st.selectbox(
            "結果コード",
            [""] + RESULT_CODE_OPTIONS,
            index=([""] + RESULT_CODE_OPTIONS).index("A 成立") if st.session_state.get("sales_from_visit") else 0,
        )
        insurance_type = st.selectbox(
            "保険種類",
            [""] + ALL_INSURANCE_OPTIONS,
            index=([""] + ALL_INSURANCE_OPTIONS).index(st.session_state.get("sales_insurance_type", "")) if st.session_state.get("sales_insurance_type", "") in [""] + ALL_INSURANCE_OPTIONS else 0,
        )
        carrier_type = st.selectbox(
            "AIG / 他社",
            CARRIER_OPTIONS,
            index=CARRIER_OPTIONS.index(st.session_state.get("sales_carrier_type", "AIG")) if st.session_state.get("sales_carrier_type", "AIG") in CARRIER_OPTIONS else 0,
        )
        renewal_month = st.selectbox(
            "更新月",
            list(range(0, 13)),
            index=normalize_renewal_month(st.session_state.get("sales_renewal_month", 0)),
            format_func=renewal_label,
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
                str(input_date),
                company_name,
                staff,
                new_cases,
                teleapo,
                visits,
                contracts,
                sonpo_premium,
                sonpo_commission,
                seiho_new_s,
                seiho_commission,
                inforce_premium,
                result_code,
                insurance_type,
                carrier_type,
                normalize_renewal_month(renewal_month),
                memo,
            ))
            st.success("保存しました。")
            st.session_state["sales_from_visit"] = False
            st.session_state["sales_company"] = ""
            st.session_state["sales_insurance_type"] = ""
            st.session_state["sales_carrier_type"] = "AIG"
            st.session_state["sales_renewal_month"] = 0
            st.rerun()


def show_visit_page(customers_df, customer_names, user_name):
    st.subheader("訪問履歴")

    activity_df = pd.read_sql_query("SELECT * FROM activity_log ORDER BY activity_id DESC", get_conn()).fillna("")
    tab1, tab2 = st.tabs(["履歴追加", "履歴一覧"])

    with tab1:
        if not customer_names:
            st.info("先に顧客を登録してください。")
        else:
            activity_customer = st.selectbox("顧客", customer_names)
            customer_row = customers_df[customers_df["company_name"] == activity_customer].iloc[0]
            quick_type = st.radio("活動種別", ACTIVITY_TYPE_OPTIONS, horizontal=True)
            activity_date = st.date_input("日付", value=date.today(), key="visit_activity_date")
            activity_staff = st.text_input("担当者", value=user_name)
            activity_insurance = st.selectbox("提案商品", [""] + ALL_INSURANCE_OPTIONS)
            activity_memo = st.text_input("内容メモ")
            activity_next_action = st.text_input("次回アクション")
            activity_next_date = st.date_input("次回予定日", value=date.today(), key="visit_next_date")
            activity_result = st.radio("成果", RESULT_OPTIONS, horizontal=True)
            activity_temp = st.radio("温度感", TEMPERATURE_OPTIONS, horizontal=True)

            if st.button("履歴を保存", use_container_width=True):
                insert_activity((
                    str(activity_date),
                    int(customer_row["customer_id"]),
                    activity_customer,
                    quick_type,
                    activity_staff,
                    activity_insurance,
                    activity_memo,
                    activity_next_action,
                    str(activity_next_date),
                    activity_result,
                    activity_temp,
                ))
                st.success("訪問履歴を保存しました。")
                st.rerun()

    with tab2:
        if activity_df.empty:
            st.info("まだ履歴がありません。")
        else:
            st.dataframe(activity_df, use_container_width=True)


def show_opportunity_page(customers_df, opportunities_df, customer_names):
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
            opp_renewal_month = st.selectbox("更新月", list(range(0, 13)), format_func=renewal_label)
            opp_probability = st.slider("確度（%）", 0, 100, 50)
            opp_memo = st.text_input("備考")

            if st.button("案件を保存", use_container_width=True):
                insert_opportunity((
                    int(customer_row["customer_id"]),
                    opp_customer,
                    opp_insurance,
                    opp_status,
                    opp_premium,
                    opp_commission,
                    normalize_renewal_month(opp_renewal_month),
                    opp_probability,
                    opp_memo,
                ))
                st.success("案件を保存しました。")
                st.rerun()

    with tab2:
        if opportunities_df.empty:
            st.info("案件なし")
        else:
            display_opp_df = opportunities_df.copy()
            display_opp_df["renewal_month"] = display_opp_df["renewal_month"].apply(renewal_label)
            st.dataframe(display_opp_df, use_container_width=True)


def show_dm_page(customers_df, activity_df, dm_history_df, customer_names, user_name):
    st.subheader("DM発送履歴")
    tab1, tab2, tab3 = st.tabs(["DM追加", "DM一覧", "未接触アラート"])

    with tab1:
        if customer_names:
            dm_customer = st.selectbox("顧客", customer_names)
            customer_row = customers_df[customers_df["company_name"] == dm_customer].iloc[0]
            dm_send_date = st.date_input("発送日", value=date.today(), key="dm_send_date")
            dm_type = st.radio("DM種類", DM_TYPE_OPTIONS, horizontal=True)
            dm_title = st.text_input("タイトル")
            dm_memo = st.text_input("メモ")
            dm_staff = st.text_input("担当者", value=user_name)
            dm_followup_due_date = st.date_input("フォロー予定日", value=date.today() + timedelta(days=14), key="dm_followup_due_date")
            dm_followup_done = st.selectbox("フォロー対応", ["未対応", "済"])
            dm_reaction = st.selectbox("反応", DM_REACTION_OPTIONS)

            if st.button("DM履歴を保存", use_container_width=True):
                insert_dm((
                    str(dm_send_date),
                    int(customer_row["customer_id"]),
                    dm_customer,
                    dm_type,
                    dm_title,
                    dm_memo,
                    dm_staff,
                    str(dm_followup_due_date),
                    dm_followup_done,
                    dm_reaction,
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


def show_insurance_status_page(customers_df, insurance_status_df, customer_names):
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
                    int(customer_row["customer_id"]),
                    status_customer,
                    status_insurance,
                    status_progress,
                    status_memo,
                ))
                st.success("加入状況を保存しました。")
                st.rerun()

    with tab2:
        if insurance_status_df.empty:
            st.info("データなし")
        else:
            st.dataframe(insurance_status_df, use_container_width=True)


def show_address_print_page(customers_df):
    st.subheader("宛名印刷")
    if customers_df.empty:
        st.info("顧客データがありません。")
        return

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


def show_dashboard_page(kpi_df, visit_logs_df):
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

        st.write("### 月次推移")
        st.dataframe(monthly, use_container_width=True)

    st.write("### 結果コード集計")
    if kpi_df.empty:
        st.info("KPIデータなし")
    else:
        results_df = kpi_df.copy()
        results_df["result_code"] = results_df["result_code"].replace("", "未設定")
        result_summary = results_df.groupby("result_code", as_index=False).size().rename(columns={"size": "件数"})
        st.dataframe(result_summary, use_container_width=True)

    st.write("### 訪問結果集計")
    if visit_logs_df.empty:
        st.info("訪問ログなし")
    else:
        visit_summary = visit_logs_df.copy()
        visit_summary["result_code"] = visit_summary["result_code"].replace("", "未設定")
        grouped = visit_summary.groupby("result_code", as_index=False).agg({
            "visit_id": "count",
            "duration_minutes": "sum",
        }).rename(columns={"visit_id": "件数", "duration_minutes": "滞在分数合計"})
        st.dataframe(grouped, use_container_width=True)


def show_monthly_page(kpi_df):
    st.subheader("月次集計")
    monthly = to_monthly(kpi_df)
    if monthly.empty:
        st.info("まだデータがありません。")
    else:
        st.dataframe(monthly, use_container_width=True)


def show_target_page():
    target_df = pd.read_sql_query("SELECT * FROM targets WHERE id=1", get_conn()).fillna("")
    target = target_df.iloc[0]

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
                new_cases_target,
                teleapo_target,
                visits_target,
                contracts_target,
                sonpo_premium_target,
                sonpo_commission_target,
                seiho_new_s_target,
                seiho_commission_target,
                inforce_premium_target,
            ))
            st.success("目標を保存しました。")
            st.rerun()


def show_forecast_page(kpi_df):
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


def show_backup_page():
    st.subheader("バックアップ")
    st.info("安定版: ローカルバックアップ + DBダウンロード")

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


def show_mobile_home_page(customers_df, activity_df, dm_history_df, todos_df):
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
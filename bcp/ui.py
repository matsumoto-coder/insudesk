import streamlit as st

from bcp.config import IDEAL_DAYS, MAX_DAYS, FACE_RULES
from bcp.hazard_ehime import (
    build_hazard_comment,
    detect_nankai_mode,
    estimate_shutdown_days_by_ehime_hazard,
)


def get_power_face(percent: int) -> str:
    for lower, upper, face in FACE_RULES:
        if lower <= percent <= upper:
            return face
    return "😐"


def render_power_gauge(survival_days: int, percent: int):
    capped_days = min(max(survival_days, 0), MAX_DAYS)
    bar_len = 20
    filled = int((capped_days / MAX_DAYS) * bar_len) if MAX_DAYS > 0 else 0
    bar = "■" * filled + "□" * (bar_len - filled)

    st.write("#### 防災資金パワー")
    st.markdown(f"### {get_power_face(percent)} {capped_days}日 / {percent}%")
    st.code(f"0 ───── 90 ───── 180\n[{bar}]")

    if survival_days < IDEAL_DAYS:
        st.warning(f"理想90日まであと{IDEAL_DAYS - survival_days}日不足")
    elif survival_days >= 150:
        st.success("かなり強い資金耐久")
    else:
        st.success("理想ライン達成")


def render_bcp_result_summary(bcp_result: dict):
    st.markdown(
        f"""
        <div class="section-box">
            <h4>{bcp_result.get('priority_color', '🟡')} 総合判定：{bcp_result.get('level', '要確認')}</h4>
            <p><b>BCPスコア：</b>{bcp_result.get('score', 0)}点</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.write("#### 判定理由")
        for reason in bcp_result.get("reasons", []):
            st.write(f"・{reason}")

    with col2:
        st.write("#### 推奨対応")
        for rec in bcp_result.get("recommendations", []):
            st.write(f"・{rec}")


def render_bcp_section(customer_row):
    from app_core import run_bcp_assessment

    st.write("### BCP診断")

    bcp_result = run_bcp_assessment(customer_row)
    render_bcp_result_summary(bcp_result)

    is_nankai = detect_nankai_mode(customer_row)
    shutdown_days = estimate_shutdown_days_by_ehime_hazard(customer_row)
    comment = build_hazard_comment(customer_row)

    st.write("#### ハザード簡易判定")
    st.write(f"・想定停止日数: {shutdown_days}日")
    st.write(f"・南海トラフ180日モード: {'有効' if is_nankai else '無効'}")
    st.caption(comment)

    with st.expander("BCP診断メモ"):
        st.caption("※ 現在は簡易診断です。後で住所ベースの愛媛ハザード判定や資金不足額グラフを追加できます。")
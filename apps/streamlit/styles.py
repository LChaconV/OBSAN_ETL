"""
styles.py — Estilos globales de Streamlit.
"""

from __future__ import annotations

import streamlit as st


def apply_global_styles(*, map_layout: bool = False, compact_top: bool = False) -> None:
    """Aplica una apariencia clara y estable aunque el navegador use modo oscuro."""
    top_padding = "1rem" if compact_top else "2rem"
    right_panel_scroll = """
    [data-testid="column"]:last-child {
        overflow-y: auto;
        max-height: 620px;
    }
    """ if map_layout else ""

    st.markdown(
        f"""
        <style>
            :root {{
                color-scheme: light;
            }}

            #MainMenu {{ visibility: hidden; }}
            footer {{ visibility: hidden; }}


            html,
            body,
            .stApp,
            [data-testid="stAppViewContainer"],
            [data-testid="stHeader"] {{
                background: #f7f8fa !important;
                color: #111827 !important;
            }}

            [data-testid="stHeader"] {{
                background: rgba(247, 248, 250, 0.92) !important;
            }}

            .block-container {{
                padding-top: {top_padding} !important;
                padding-bottom: 0 !important;
                padding-left: 0.5rem !important;
                padding-right: 0.5rem !important;
            }}

            .stApp,
            .stApp p,
            .stApp li,
            .stApp label,
            .stApp span,
            .stMarkdown,
            [data-testid="stMarkdownContainer"],
            [data-testid="stWidgetLabel"],
            [data-testid="stStatusWidget"],
            [data-testid="stMetric"],
            [data-testid="stMetric"] * {{
                color: #111827 !important;
            }}

            .stCaption,
            [data-testid="stCaptionContainer"],
            small {{
                color: #4b5563 !important;
            }}

            [data-testid="stSidebar"] {{
                background: #e8e8ea !important;
            }}

            [data-testid="stSidebar"] *,
            [data-testid="stSidebar"] label,
            [data-testid="stSidebar"] h1,
            [data-testid="stSidebar"] h2,
            [data-testid="stSidebar"] h3 {{
                color: #111827 !important;
            }}

            [data-testid="stSidebar"] .stCaption,
            [data-testid="stSidebar"] small {{
                color: #4b5563 !important;
            }}

            [data-testid="stSidebar"] hr,
            hr {{
                border-color: #d1d5db !important;
            }}

            iframe {{
                display: block;
            }}

            input,
            textarea,
            [data-baseweb="input"] *,
            [data-baseweb="select"] *,
            [data-baseweb="textarea"] * {{
                color: #111827 !important;
            }}

            [data-baseweb="input"],
            [data-baseweb="select"] > div,
            [data-baseweb="textarea"] {{
                background: #ffffff !important;
                border-color: #d1d5db !important;
            }}

            div[data-testid="stExpander"],
            div[data-testid="stStatusWidget"],
            [data-testid="stFileUploaderDropzone"] {{
                background: #ffffff !important;
                color: #111827 !important;
                border-color: #d1d5db !important;
            }}

            div[data-testid="stAlert"] {{
                color: #111827 !important;
            }}

            pre,
            code {{
                color: #111827 !important;
                background: #f3f4f6 !important;
            }}

            button {{
                color: #111827 !important;
            }}

            button[kind="primary"],
            button[data-testid="baseButton-primary"] {{
                background: #1d4ed8 !important;
                color: #ffffff !important;
                border-color: #1d4ed8 !important;
            }}

            button[kind="primary"] *,
            button[data-testid="baseButton-primary"] * {{
                color: #ffffff !important;
            }}

            button:disabled,
            button:disabled * {{
                color: #6b7280 !important;
            }}

            {right_panel_scroll}
        </style>
        """,
        unsafe_allow_html=True,
    )

"""Central place for Streamlit session state keys used across pages."""
from __future__ import annotations

# Global reset flag used when the user wants to restart the flow.
FULL_RESET_FLAG = "_do_full_reset"

# Solicitation page keys
REQUEST_LINES = "lines_df"
REQUEST_EDITOR_KEY = "editor_lines_v2"
UTD_BASE_SELECTION = "utd_base_sel"
SUCCESS_QUANTITY = "success_qtd"
SUCCESS_NAME = "success_nome"
SUCCESS_EMAIL = "success_email"
SUCCESS_RESUMO = "success_resumo"
NAME_INPUT = "nome_input"
EMAIL_INPUT = "email_input"
UTD_SELECTION = "utds_sel"
TURMA_SELECTION = "turma_sel"

# Resumo page keys
RESUMO_RESET = "reset_filters_resumo"
RESUMO_DATE_FILTER = "f_data_resumo"
RESUMO_UTD_FILTER = "f_utd_resumo"
RESUMO_BASE_FILTER = "f_base_resumo"
RESUMO_EMAIL_FILTER = "f_email_resumo"

# Admin page keys
ADMIN_RESET = "reset_filters_admin"
ADMIN_DATE_FILTER = "f_data_admin"
ADMIN_UTD_FILTER = "f_utd_admin"
ADMIN_BASE_FILTER = "f_base_admin"
ADMIN_EMAIL_FILTER = "f_email_admin"
ADMIN_STATUS_FILTER = "f_status_admin"
ADMIN_EDITOR_KEY = "admin_editor_v2"
ADMIN_PENDING_CHANGES = "admin_pending_changes"
ADMIN_INDEX_TO_KEY = "admin_index_to_key"
ADMIN_LAST_APPLY = "admin_last_apply_success"
CSV_SELECTION = "csv_row_selection"
ADMIN_EMAIL = "admin_email"

# Authentication keys (Streamlit Authenticator defaults)
AUTH_STATUS = "authentication_status"
USERNAME = "username"

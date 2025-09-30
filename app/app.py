from __future__ import annotations

import streamlit as st

from app.settings import PAGE_ICON, PAGE_LAYOUT, PAGE_TITLE
from app.state.session import handle_full_reset


def configure_page() -> None:
    st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout=PAGE_LAYOUT)


def main() -> None:
    configure_page()
    handle_full_reset()

    st.title(PAGE_TITLE)
    st.caption("Solicite a geração de notas por UTD, TURMA e BASE com validações operacionais de horário.")
    st.markdown(
        """
        Use o menu lateral para navegar pelas seções:

        * **Solicitar Geração** — envio de novas solicitações para o HANA.
        * **Resumo de Pedidos** — consulta dos pedidos com filtros rápidos.
        * **Gestão de Pedidos** — área restrita para administração, alteração de status e exportação de CSV.
        """
    )

    with st.sidebar:
        st.header("Como usar o painel")
        st.markdown(
            """
            1. Preencha seu nome e e-mail corporativo (@neoenergia.com).
            2. Escolha uma ou mais UTDs e selecione a TURMA.
            3. Para cada UTD, selecione as BASEs desejadas.
            4. Configure os serviços, pacotes, geração e justificativas na tabela.
            5. Clique em **Enviar Solicitação** para registrar o pedido.
            """
        )
        st.divider()
        st.caption(
            "Dúvidas ou falhas? João Paulo (joao.almeida@neoenergia.com) ou Luiz Felipe (luiz.espozel@neoenergia.com)."
        )
        st.divider()
        st.caption("Feito por: Pedro Azevedo (pedro.azevedo@neoenergia.com)")


if __name__ == "__main__":  # pragma: no cover - Streamlit entry-point
    main()

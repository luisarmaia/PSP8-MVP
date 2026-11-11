# Modelo de dados: esquema estrela

O fato é a **etapa de voo**. A dimensão de condição meteorológica é usada em dois papéis (*role-playing*): na origem, no horário previsto de partida, e no destino, no horário previsto de chegada. O mesmo acontece com a dimensão de aeroporto.

```mermaid
erDiagram
    fato_voo }o--|| dim_tempo : "sk_data"
    fato_voo }o--|| dim_horario : "sk_hora_partida"
    fato_voo }o--|| dim_companhia : "sk_companhia"
    fato_voo }o--|| dim_aeroporto : "sk_aeroporto_origem"
    fato_voo }o--|| dim_aeroporto : "sk_aeroporto_destino"
    fato_voo }o--|| dim_condicao_meteorologica : "sk_condicao_origem"
    fato_voo }o--|| dim_condicao_meteorologica : "sk_condicao_destino"

    fato_voo {
        string sk_voo PK
        int sk_data FK
        int sk_hora_partida FK
        string sk_companhia FK
        string sk_aeroporto_origem FK
        string sk_aeroporto_destino FK
        int sk_condicao_origem FK
        int sk_condicao_destino FK
        boolean cancelado
        double atraso_chegada_min
        boolean atrasado_15
        boolean atrasado_30
        boolean em_escopo
        string cenario_clima
    }
    dim_tempo {
        int sk_data PK
        date data
        int ano
        int mes
        string dia_semana
    }
    dim_horario {
        int sk_hora PK
        string faixa_horaria
    }
    dim_companhia {
        string sk_companhia PK
        string nome
    }
    dim_aeroporto {
        string sk_aeroporto PK
        string nome
        string uf
        string regiao
        boolean tem_metar
    }
    dim_condicao_meteorologica {
        int sk_condicao PK
        string descricao
        boolean tempo_adverso
        boolean fl_visibilidade_baixa
        boolean fl_teto_baixo
        boolean fl_trovoada
        boolean fl_chuva_forte
        boolean fl_rajada_forte
    }
```

O diagrama mostra os atributos principais; a lista completa está em [`catalogo_dados.md`](catalogo_dados.md).

## Por que estrela, e não *flat*
As perguntas P1–P5 cortam a mesma medida (atraso) por aeroporto, horário, mês, companhia e condição meteorológica. Com dimensões separadas, cada corte é uma junção simples, e a condição meteorológica é descrita uma única vez (33 linhas) em vez de repetida em ~2 milhões de etapas.

## Código da condição meteorológica
`sk_condicao` é a soma dos pesos dos fatores presentes na observação METAR aplicável:

| Fator | Peso |
|---|---|
| Visibilidade < 5 km | 1 |
| Teto < 1.500 ft | 2 |
| Trovoada (TS, VCTS) | 4 |
| Chuva forte (+RA, +SHRA, +TSRA) | 8 |
| Rajada ≥ 25 kt | 16 |

Exemplo: `5` = visibilidade baixa + trovoada. `0` = tempo bom. `-1` = sem observação válida nos 90 min anteriores.

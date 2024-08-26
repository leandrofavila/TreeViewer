import pandas as pd
import cx_Oracle


class DB:
    def __init__(self):
        self.db_connection = None

    @staticmethod
    def get_connection():
        dsn = cx_Oracle.makedsn("10.40.3.10", 1521, service_name="f3ipro")
        connection = cx_Oracle.connect(user=r"focco_consulta", password=r'consulta3i08', dsn=dsn, encoding="UTF-8")
        cur = connection.cursor()
        return cur


    def geral(self, carregamento):
        cur = self.get_connection()
        cur.execute(
            r"SELECT CAR.DESCRICAO, EMP.COD_ITEM AS MAE, PLA.COD_ITEM AS FILHO, TOR.NUM_ORDEM, CARIT.QTDE, TOR.TIPO_ORDEM, "
            r"LISTAGG(OP.DESCRICAO ||'('|| CASE WHEN MOV.ID IS NULL THEN 'PENDENTE' ELSE 'APONTADO' ||':'||MOV.QUANTIDADE END || ')', '|') WITHIN GROUP (ORDER BY ROT.SEQ) "
            r"FROM FOCCO3I.TITENS_PDV ITPDV "
            r"INNER JOIN FOCCO3I.TPEDIDOS_VENDA PDV                       ON PDV.ID = ITPDV.PDV_ID "
            r"INNER JOIN FOCCO3I.TITENS_COMERCIAL CO                      ON CO.ID = ITPDV.ITCM_ID "
            r"INNER JOIN FOCCO3I.TITENS_ENGENHARIA ENG                    ON ENG.ITEMPR_ID = CO.ITEMPR_ID "
            r"INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS_IT CARIT     ON CARIT.ITPDV_ID = ITPDV.ID "
            r"INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR          ON CAR.ID = CARIT.SR_CARREG_ID "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP                          ON EMP.ID = CO.ITEMPR_ID "
            r"INNER JOIN FOCCO3I.TORDENS_VINC_ITPDV VINC                  ON ITPDV.ID = VINC.ITPDV_ID "
            r"INNER JOIN FOCCO3I.TORDENS TOR                              ON VINC.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TITENS_PLANEJAMENTO PLA                  ON PLA.ID = TOR.ITPL_ID "
            r"INNER JOIN FOCCO3I.TORDENS_ROT ROT                          ON ROT.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TOPERACAO OP                             ON OP.ID = ROT.OPERACAO_ID "
            r"LEFT JOIN FOCCO3I.TORDENS_MOVTO MOV                         ON MOV.TORDEN_ROT_ID = ROT.ID "
            r"WHERE CAR.CARREGAMENTO IN (" + str(carregamento) + ") "
            r"AND ENG.TP_ITEM = 'F' "
            r"AND ITPDV.SIT_PROC = 'LIB' "
            r"GROUP BY CAR.DESCRICAO, EMP.COD_ITEM, PLA.COD_ITEM, TOR.NUM_ORDEM, CARIT.QTDE, TOR.TIPO_ORDEM "
        )
        df_geral = pd.DataFrame(cur.fetchall(), columns=["DESC_CAR", "MAE", "FILHO", "NUM_ORDEM",
                                                         "QTDE", "TIPO_ORDEM", "SITUACAO"])

        cur.close()
        return df_geral

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

    def car(self, carregamento):
        cur = self.get_connection()
        cur.execute(
            r"SELECT DISTINCT TPL.COD_ITEM,  "
            r"TOR.NUM_ORDEM, "
            r"TOR.QTDE,  "
            r"TOR.tipo_ordem, "
            r"TIT.DESC_TECNICA, "
            r"TFUN.NOME AS PLANEJADOR, "
            r"PDV.NUM_ITEM "
            r"FROM FOCCO3I.TITENS_PLANEJAMENTO TPL "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP          ON TPL.ITEMPR_ID = EMP.ID "
            r"INNER JOIN FOCCO3I.TITENS TIT               ON EMP.ITEM_ID = TIT.ID  "
            r"INNER JOIN FOCCO3I.TORDENS TOR              ON TPL.ID = TOR.ITPL_ID "
            r"INNER JOIN FOCCO3I.TDEMANDAS TDE            ON TOR.ID = TDE.ORDEM_ID "
            r"INNER JOIN FOCCO3I.TORDENS_ROT ROT          ON TOR.ID = ROT.ORDEM_ID "
            r"INNER JOIN FOCCO3I.TORD_ROT_FAB_MAQ FAB     ON ROT.ID = FAB.TORDEN_ROT_ID "
            r"INNER JOIN FOCCO3I.TMAQUINAS MAQ            ON FAB.MAQUINA_ID = MAQ.ID "
            r"INNER JOIN FOCCO3I.TITENS_PLAN_FUNC PLA     ON TPL.ID = PLA.ITPL_ID "
            r"INNER JOIN FOCCO3I.TFUNCIONARIOS TFUN       ON PLA.FUNC_ID = TFUN.ID "
            r"INNER JOIN FOCCO3I.TORDENS_VINC_ITPDV IT    ON IT.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TITENS_PDV PDV           ON PDV.ID = IT.ITPDV_ID "
            r"WHERE TOR.ID IN( "
            r"                SELECT TOR.ID "
            r"                FROM FOCCO3I.TORDENS TOR "
            r"                INNER JOIN FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC      ON TOR.ID = VINC.ORDEM_ID "
            r"                INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR  ON VINC.CARERGAM_ID = CAR.ID "
            r"                WHERE CAR.CARREGAMENTO IN (" + str(carregamento) + ") "
                                                                                 r"                ) "
                                                                                 r"GROUP BY TPL.COD_ITEM, TOR.NUM_ORDEM, TOR.QTDE, TIT.DESC_TECNICA, TFUN.NOME, TOR.tipo_ordem, PDV.NUM_ITEM "
        )
        df = cur.fetchall()
        df = pd.DataFrame(df, columns=['COD_ITEM', 'NUM_ORDEM', 'QTDE', 'TIPO_ORDEM', 'DESC_TECNICA', 'PLANEJADOR',
                                       'NUM_ITEM'])
        df['QTDE'] = df['QTDE'].astype(int)
        # df.to_csv(f"carregamento{carregamento}.csv", index=False)
        return df

    def filhos(self, carregamento, ordem):
        cur = self.get_connection()
        cur.execute(
            r"SELECT DISTINCT OPFI.NUM_ORDEM, TPL.COD_ITEM, OPPAI.DT_EMISSAO, OPFI.TIPO_ORDEM, OPFI.QTDE, "
            r"LISTAGG(OP.DESCRICAO ||'  - '|| CASE WHEN MOV.ID IS NULL THEN 'PENDENTE' ELSE 'APONTADO' ||': '||MOV.QUANTIDADE END || '', '|') WITHIN GROUP (ORDER BY ROT.SEQ) "
            r"FROM FOCCO3I.TORDENS OPPAI "
            r"INNER JOIN FOCCO3I.TDEMANDAS TDE                    ON TDE.ORDEM_ID = OPPAI.ID "
            r"INNER JOIN FOCCO3I.TORDENS OPFI                     ON OPFI.ITPL_ID = TDE.ITPL_ID "
            r"INNER JOIN FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC      ON VINC.ORDEM_ID = OPFI.ID "
            r"INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR  ON CAR.ID = VINC.CARERGAM_ID "
            r"INNER JOIN FOCCO3I.TITENS_PLANEJAMENTO TPL          ON TPL.ID = OPFI.ITPL_ID "
            r"INNER JOIN FOCCO3I.TORDENS_ROT ROT                  ON ROT.ORDEM_ID = OPFI.ID "
            r"INNER JOIN FOCCO3I.TOPERACAO OP                     ON OP.ID = ROT.OPERACAO_ID "
            r"LEFT JOIN FOCCO3I.TORDENS_MOVTO MOV                 ON MOV.TORDEN_ROT_ID = ROT.ID "
            r"WHERE OPPAI.NUM_ORDEM IN (" + str(ordem) + ") "
            r"AND CAR.CARREGAMENTO = " + str(carregamento) + " "
            r"GROUP BY OPFI.NUM_ORDEM, TPL.COD_ITEM, OPPAI.DT_EMISSAO, OPFI.TIPO_ORDEM, OPFI.QTDE "
        )
        df = pd.DataFrame(cur.fetchall(), columns=["NUM_ORDEM", "COD_ITEM", "DT_EMISSAO", "TIPO_ORDEM", "QTDE",
                                                   "LISTAGG"]).astype(int, errors="ignore")
        cur.close()
        # df.to_csv(f"dados_filhos{carregamento, ordem}.csv", index=False)
        return df

    def pop_up(self, carregamento):
        cur = self.get_connection()
        cur.execute(
            r"SELECT  "
            r"TOR.NUM_ORDEM, "
            r"LISTAGG(OP.DESCRICAO ||'  - '|| CASE WHEN MOV.ID IS NULL THEN 'PENDENTE' ELSE 'APONTADO' ||': '||MOV.QUANTIDADE END || '', '|') WITHIN GROUP (ORDER BY ROT.SEQ) "
            r"FROM FOCCO3I.TORDENS TOR "
            r"INNER JOIN FOCCO3I.TORDENS_VINC_ITPDV VINC          ON VINC.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TITENS_PDV ITPDV                 ON ITPDV.ID = VINC.ITPDV_ID "
            r"INNER JOIN FOCCO3I.TPEDIDOS_VENDA PDV               ON PDV.ID = ITPDV.PDV_ID "
            r"INNER JOIN FOCCO3I.TORDENS_ROT ROT                  ON ROT.ORDEM_ID = TOR.ID "
            r"INNER JOIN FOCCO3I.TOPERACAO OP                     ON OP.ID = ROT.OPERACAO_ID "
            r"LEFT JOIN FOCCO3I.TORDENS_MOVTO MOV                 ON MOV.TORDEN_ROT_ID = ROT.ID "
            r"WHERE (SELECT COUNT(CAR.ID)  "
            r"    FROM FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC       "
            r"    INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR   ON VINC.CARERGAM_ID = CAR.ID "
            r"    WHERE TOR.ID = VINC.ORDEM_ID "
            r"    AND CAR.carregamento = " + str(carregamento) + ") > 0 "
            r"GROUP BY TOR.NUM_ORDEM  "
        )
        pop_up = pd.DataFrame(cur.fetchall(), columns=["NUM_ORDEM", "LISTAGG"])
        cur.close()
        # pop_up.to_csv(f"pop_up{carregamento}.csv", index=False)
        return pop_up

    def mach_load(self):
        cur = self.get_connection()
        cur.execute(
            r"SELECT  "
            r"    MAQ.DESCRICAO, "
            r"    COUNT(ROT.ID) OP_PENDENTES, "
            r"    SUM(CASE WHEN MOV.QUANTIDADE IS NULL THEN TOR.QTDE ELSE (TOR.QTDE - MOV.QUANTIDADE) END ) AS PEÇAS_PENDENTES "
            r"FROM FOCCO3I.TORDENS TOR "
            r"    INNER JOIN FOCCO3I.TORDENS_ROT ROT                  ON ROT.ORDEM_ID = TOR.ID "
            r"    INNER JOIN FOCCO3I.TORD_ROT_FAB_MAQ RMAQ            ON RMAQ.TORDEN_ROT_ID = ROT.ID "
            r"    INNER JOIN FOCCO3I.TMAQUINAS MAQ                    ON MAQ.ID = RMAQ.MAQUINA_ID "
            r"    LEFT JOIN FOCCO3I.TORDENS_MOVTO MOV                 ON MOV.TORDEN_ROT_ID = ROT.ID "
            r"WHERE TOR.TIPO_ORDEM <> 'OFE' "
            r"AND (MOV.ID IS NULL OR  "
            r"        (SELECT SUM(APONT.QUANTIDADE) FROM FOCCO3I.TORDENS_MOVTO APONT "
            r"        WHERE APONT.TORDEN_ROT_ID = ROT.ID) < TOR.QTDE) "
            r"GROUP BY MAQ.DESCRICAO "
        )
        mach_load = pd.DataFrame(cur.fetchall(), columns=["MAQUINA", "OP_PENDENTES", "PECAS_PENDENTES"])
        cur.close()
        return mach_load


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

WITH ff AS
  (SELECT dia,
    fk_unidade,
    S_FERIADO.verificaFeriado(dia, fk_unidade,'WFM') fer_next_day
  FROM
    (SELECT to_date(:d1,'yyyy-mm-dd') + rownum -1 AS dia,
      :l FK_UNIDADE
    FROM all_objects
    WHERE rownum <= to_date(:d2,'yyyy-mm-dd')-to_date(:d1,'yyyy-mm-dd')+1
    )
  )
SELECT :s AS fk_Secao,
  ff.dia as DATA,
  TO_CHAR(ff.dia, 'D') DIA_SEMANA,
  ff.fer_next_day,
  S_SECAO.getFaixa(:s, ff.dia, (
  CASE
    WHEN
      /*p_feriado*/
      ff.fer_next_day = 'N'
    THEN TO_CHAR(ff.dia, 'DY', 'nls_date_language=portuguese')
    ELSE 'FER'
  END), 'INI', 'S') INI,
  S_SECAO.getFaixa(:s, ff.dia, (
  CASE
    WHEN
      /*p_feriado*/
      ff.fer_next_day = 'N'
    THEN TO_CHAR(ff.dia, 'DY', 'nls_date_language=portuguese')
    ELSE 'FER'
  END), 'FIM', 'S') FIM
FROM ff
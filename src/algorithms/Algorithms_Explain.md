# Introdução

A atribuição de folgas é um problema clássico de planeamento e otimização que, apesar de parecer simples, cresce rapidamente em complexidade quando se consideram regras reais: limites contratuais, janelas mínimas de descanso, equilíbrio entre colaboradores, preferências, máximos de folgas consecutivas, entre outros. Em termos combinatórios, o número de soluções possíveis aumenta exponencialmente com o número de trabalhadores e de dias, tornando inviável uma abordagem por tentativa e erro.  

É neste contexto que o **Constraint Programming (CP)**, através do **OR-Tools CP-SAT**, se torna uma ferramenta poderosa. No CP, o problema é modelado de forma declarativa: definem-se variáveis, domínios e restrições, e o solver é responsável por explorar o espaço de soluções, encontrando aquelas que cumprem todas as condições impostas e, quando necessário, otimizando objetivos adicionais. Este paradigma ajusta-se naturalmente à atribuição de folgas, já que muitas das regras envolvidas são discretas e combinatórias.  

O funcionamento do CP-SAT pode ser compreendido em duas grandes fases interligadas: a **redução do espaço de solução** e a **exploração inteligente do espaço viável**. Dentro da redução do espaço de solução, o solver realiza um processo de *presolve* que simplifica o modelo, fixa valores evidentes e elimina redundâncias, reduzindo significativamente a dimensão do problema. Em seguida, aplica **propagação de restrições**, que consiste em deduzir automaticamente implicações sempre que variáveis são fixadas, restringindo o espaço de procura. Quando combinações levam a contradições, o solver utiliza **aprendizagem de conflitos**, armazenando cláusulas que impedem revisitar becos sem saída.  

Na exploração do espaço viável, o CP-SAT utiliza **heurísticas de branching** para decidir que variáveis atribuir primeiro, executa **buscas em paralelo** com diferentes estratégias, aplica **relaxações lineares** para obter limites sobre o objetivo e recorre a **Large Neighborhood Search (LNS)** para melhorar soluções parcialmente fixas. Além disso, o solver faz **restarts controlados** para evitar ficar preso em regiões complexas do espaço e permite **enumerar múltiplas soluções viáveis**, abrindo a possibilidade de analisar diferentes cenários em detalhe.  

Apesar de existir apenas um solver em execução, é possível estruturar a busca de forma a simular uma decomposição por trabalhador. Técnicas como LNS, alternância de otimização entre grupos ou execuções paralelas com *seeds* distintas permitem explorar diferentes regiões do espaço, como se cada trabalhador tivesse o seu próprio “solver virtual”, mas garantindo sempre a consistência global do plano.  

Desta forma, o **OR-Tools CP-SAT** combina técnicas de simplificação agressiva com métodos avançados de exploração, produzindo soluções de elevada qualidade para problemas de atribuição de folgas. A sua capacidade de equilibrar eficiência, flexibilidade e riqueza de soluções torna-o uma abordagem particularmente adequada para lidar com a complexidade deste tipo de planeamento.  

---

# Modelo de Dados

O algoritmo é alimentado por 3 **dataframes**, que vêm na variável `medium_data`, e um dicionário de parâmetros, `algorithm_treatment_params`. Estes 3 dataframes são:

## 1. `df_colaborador`

Esta matriz tem representada toda a informação relativa a um colaborador e necessária para o algoritmo, desde nome, id, tipo de contrato a número mínimo de folgas a atribuir.  

As colunas são as seguintes:

1. `unidade`  
2. `secao`  
3. `posto`  
4. `fk_colaborador`  
5. `matricula`: identificador do colaborador no algoritmo  
6. `out`  
7. `tipo_contrato`: número máximo de dias de trabalho numa semana  
8. `ciclo`: se for do tipo “completo”, já tem as folgas predeterminadas  
9. `l_total`: número de folgas a atribuir (alcampo)  
10. `l_dom`: número de folgas em domingos e dias especiais a atribuir (alcampo)  
11. `l_d`: número de folgas de compensação por trabalho em domingos e dias especiais (alcampo)  
12. `l_q`  
13. `lqs`  
14. `c2d`: número de fins de semana de folga (sábado e domingo de folga) mínimo para a salsa  
15. `c3d`: número de fins de semana de qualidade de folga (sábado, domingo e segunda ou sexta de folga) (alcampo) 
16. `cxx`: número de dias de folga que podem estar juntos num ano (alcampo)
17. `descansos_atrb`
18. `lq_og`
19. `dofhc`: número de dias de trabalho extra em dias de folga especiais e domingos (alcampo)
20. `vx`
21. `data_admissao`
22. `data_demissao`
23. `l_dom_salsa`: número mínimo de folgas para atribuir a domingos no contexto da salsa
24. `l_res`
25. `l_res2`
26. `min_fest_h`
27. `nivel`: nivel para atribuição de folgas mutamente exclusivas

---

## 2. `df_estimativas`

Esta matriz representa as necessidades para um respetivo dia, num posto e para os diferentes tipos de turno (Manhã ou Tarde). Vai ser a força principal por detrás da procura do solver, quando este estiver a procurar reduzir as diferenças entre o número de pessoas atríbuido e o necessário.

As colunas são as seguintes:

1. `data`: data específica da estimativa
2. `media_turno`: número médio de pessoas necessárias para o turno
3. `max_turno`: número máximo de pessoas necessárias para o turno
4. `min_turno`: número mínimo de pessoas necessárias para o turno
5. `sd_turno`: desvio padrão das necessidades do turno
6. `turno`: tipo de turno (M - Manhã, T - Tarde)
7. `fk_tipo_posto`: identificador do tipo de posto
8. `data turno` : data e turno
9. `+H`
10. `aux`
11. `pess_obj`: número objetivo de pessoas para o turno (valor utilizado na otimização)
12. `diff`
13. `wday`: dia da semana (1-7)

---

## 3. `df_calendario`

Esta matriz contém o histórico e planeamento base de cada colaborador, definindo a sua disponibilidade, tipo de turno já atribuído, e dias especiais. É a matriz que conecta colaboradores com dias específicos e serve como base para validar restrições e pré-atribuições.

As colunas são as seguintes:

1. `colaborador`: identificador do colaborador (correspondente à matricula)
2. `data`: data específica do registo
3. `tipo_turno`: turno já atribuído ou estado do colaborador:
   - `M`: turno da manhã
   - `T`: turno da tarde
   - `L`: dia de folga normal
   - `L_DOM`: dia de folga em domingo
   - `A/AP`: ausência/falta
   - `V`: dia vazio (não trabalha)
   - `F`: feriado encerrado
   - `-`: dia não definido
4. `horario` : identifica para um colaborador os dias que pode trabalhar com 'H' 
5. `wday` : dia da semana (1-7 de segunda a domingo)
6. `id`
7. `ww`: número da semana no ano (1-52/53)
8. `wd`: dia da semana (Mon, Tue, Wed, Thu, Fri, Sat, Sun)
9. `dia_tipo`: classificação do tipo de dia (Mon, Tue, Wed, Thu, Fri, Sat ou domYf - domingo/feriado)
10. `emp`
11. `data_admissao`
12. `data_demissao`


---

## 4. `algorithm_treatment_params`

Dicionário contendo parâmetros de configuração do algoritmo:

- `admissao_proporcional`: string que define se deve aplicar cálculo proporcional com arredondamento para baixo (floor) ou para cima (ceil) de folgas para semanas onde os colaboradores são admitidos e/ou demitidos.

---

# Leitura e Tratamento de Dados

O processo de leitura e tratamento dos dados é executado pela função `read_data_salsa()` e representa uma das fases mais críticas do algoritmo. Cada etapa processa os dados brutos e os transforma em estruturas otimizadas para o modelo de programação por restrições.

## 1. Validação e Normalização Detalhada

### 1.1 Validação Inicial de DataFrames
```python
required_dataframes = ['df_colaborador', 'df_estimativas', 'df_calendario']
missing_dataframes = [df for df in required_dataframes if df not in medium_dataframes]
```
**Processo detalhado**:
- O algoritmo verifica se os 3 DataFrames essenciais estão presentes no dicionário `medium_dataframes`
- Se algum DataFrame estiver em falta, o algoritmo termina imediatamente com erro `ValueError`
- Esta validação previne falhas silenciosas durante a execução

### 1.2 Normalização de Colunas
```python
matriz_colaborador_gd.columns = matriz_colaborador_gd.columns.str.lower()
matriz_estimativas_gd.columns = matriz_estimativas_gd.columns.str.lower()
matriz_calendario_gd.columns = matriz_calendario_gd.columns.str.lower()
```
**Impacto na execução**:
- Garante consistência no acesso às colunas independentemente da capitalização original
- Evita erros de `KeyError` durante o processamento
- Permite compatibilidade com diferentes fontes de dados

### 1.3 Validação Granular de Colunas
**Para df_colaborador**:
```python
required_colaborador_cols = ['matricula', 'C2D', 'data_admissao', 'data_demissao','L_DOM_SALSA']
```
- `matricula`: identificador único do colaborador (chave primária)
- `c2d`: fins de semana de qualidade (sábado+domingo de folga)
- `data_admissao/data_demissao`: datas contratuais
- `l_dom_salsa`: folgas mínimas em domingos para o contexto SALSA


**Para df_calendario**:
```python
required_calendario_cols = ['colaborador', 'data', 'wd', 'dia_tipo', 'tipo_turno']
```
- `colaborador`: ligação ao df_colaborador via matricula
- `data`: data específica do registo (datetime)
- `wd`: dia da semana (Mon/Tue/Wed/Thu/Fri/Sat/Sun)
- `dia_tipo`: classificação do dia (normal/domYf)
- `tipo_turno`: estado atual do colaborador para esse dia

**Para df_estimativas**:
```python
required_estimativas_cols = ['data', 'turno', 'media_turno', 'max_turno', 'min_turno', 'pess_obj', 'sd_turno', 'fk_tipo_posto', 'wday']
```
- `data`: data da estimativa
- `turno`: tipo de turno (M/T)
- `media_turno`: necessidade média de pessoas
- `max_turno`: necessidade máxima de pessoas
- `min_turno`: necessidade mínima de pessoas
- `pess_obj`: objetivo de staffing (usado na otimização)
- `sd_turno`: desvio padrão das necessidades
- `fk_tipo_posto`: tipo de posto de trabalho
- `wday`: dia da semana numérico (1-7)

### 1.4 Conversão e Limpeza de Tipos
```python
# Conversão de colaboradores para numérico
matriz_calendario_gd['colaborador'] = pd.to_numeric(matriz_calendario_gd['colaborador'], errors='coerce')

# Conversão de datas
matriz_calendario_gd['data'] = pd.to_datetime(matriz_calendario_gd['data'], errors='coerce')
```
**Tratamento de erros**:
- Colaboradores não numéricos são convertidos para `NaN` e posteriormente removidos
- Datas inválidas são convertidas para `NaT` e geram warnings
- Registos com "TIPO_DIA" são filtrados (cabeçalhos espúrios)

## 2. Cálculo de Variáveis Derivadas Detalhado

### 2.1 Cálculo de L_Q (Folgas de Qualidade)
```python
matriz_colaborador_gd["l_q"] = (
    matriz_colaborador_gd["l_total"] - 
    matriz_colaborador_gd["l_dom"] - 
    matriz_colaborador_gd["c2d"] - 
    matriz_colaborador_gd["c3d"] - 
    matriz_colaborador_gd["l_d"] - 
    matriz_colaborador_gd["cxx"] - 
    matriz_colaborador_gd["vz"] - 
    matriz_colaborador_gd["l_res"] - 
    matriz_colaborador_gd["l_res2"]
)
```

No contexto da ALcampo, a coluna l_q pode vir com dados incorretos, logo é preciso fazer esta correção prévia.
**Lógica de cálculo**:
- **l_total**: total de folgas  (base)
- **Subtraem-se folgas já atribuídas ou reservadas**:
  - `l_dom`: folgas obrigatórias em domingos e feriados
  - `c2d`: fins de semana de qualidade de 2 dias
  - `c3d`: fins de semana de qualidade de 3 dias
  - `l_d`: compensações por trabalho em dias especiais
  - `cxx`: folgas consecutivas
  - `vz`: folgas adicionais
  - `l_res + l_res2`: folgas adicionais
- **Resultado**: folgas "livres" que podem ser atribuídas flexivelmente

**Tratamento de valores negativos**:
- Se L_Q < 0, indica sobre-atribuição de folgas específicas
- O algoritmo pode ajustar automaticamente ou gerar warnings

## 3. Identificação de Trabalhadores Válidos Detalhada

### 3.1 Classificação de Trabalhadores
```python
workers_colaborador_complete = set(matriz_colaborador_gd['matricula'].dropna().astype(int))
workers_calendario_complete = set(matriz_calendario_gd['colaborador'].dropna().astype(int))
workers_colaborador = set(matriz_colaborador_gd[matriz_colaborador_gd['ciclo'] != 'Completo']['matricula'].dropna().astype(int))
```

**Três categorias principais**:

1. **workers_colaborador_complete**: 
   - Todos os colaboradores no df_colaborador
   - Inclui trabalhadores de ciclo completo e incompleto
   - Base para validação de integridade

2. **workers_calendario_complete**:
   - Colaboradores presentes no calendário
   - Podem ter registos históricos ou pré-atribuições
   - Base para validação de existência de dados temporais

3. **workers_colaborador** (ciclo != 'Completo'):
   - Colaboradores que necessitam atribuição algorítmica de folgas
   - Excluem trabalhadores com horários pré-definidos
   - Alvos principais do algoritmo de otimização

### 3.2 Intersecção e Validação
```python
valid_workers = workers_colaborador.intersection(workers_calendario_complete)
valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)
workers_complete_cycle = sorted(set(workers_complete)-set(workers))
```

**Lógica de validação**:
- **valid_workers**: trabalhadores que precisam de atribuição E têm dados de calendário
- **valid_workers_complete**: todos os trabalhadores válidos (com dados completos)
- **workers_complete_cycle**: trabalhadores de ciclo completo (folgas pré-definidas)

### 3.3 Matrizes finais
- **workers**: trabalhadores que não tem ciclo completo
- **workers_complete**: todos os traalhadores
- **workers_complete_cycle**: trabalhadores de ciclo completo

## 4. Extração de Informação Temporal Detalhada

### 4.1 Extração de Dias do Ano
```python
days_of_year = sorted(matriz_calendario_gd['data'].dt.dayofyear.unique().tolist())
```
**Processo**:
- Converte datas para dia do ano (1-365/366)
- Remove duplicados e ordena cronologicamente
- Cria base temporal para indexação do modelo CP com todoas as datas presentes na matriz_calendario_gd

### 4.2 Identificação de Dias Especiais
```python
sundays = sorted(matriz_calendario_gd[matriz_calendario_gd['wd'] == 'Sun']['data'].dt.dayofyear.unique().tolist())

holidays = sorted(matriz_calendario_gd[
    (matriz_calendario_gd['wd'] != 'Sun') & 
    (matriz_calendario_gd["dia_tipo"] == "domYf")
]['data'].dt.dayofyear.unique().tolist())

closed_holidays = sorted(matriz_calendario_gd[
    matriz_calendario_gd['tipo_turno'] == "F"
]['data'].dt.dayofyear.unique().tolist())
```

**Categorização detalhada**:
- **sundays**: domingos regulares (wd == 'Sun')
- **holidays**: feriados que não são domingos (dia_tipo == "domYf" AND wd != 'Sun')
- **closed_holidays**: dias de encerramento forçado (tipo_turno == "F")
- **special_days**: união de sundays + holidays (para restrições especiais)

### 4.3 Parâmetros Adicionais
```python
non_holidays = [d for d in days_of_year if d not in closed_holidays]
```

```python
            matriz_calendario_sorted = matriz_calendario_gd.sort_values('data')
            first_date_row = matriz_calendario_sorted.iloc[0]

            # Get the year from the first date and create January 1st of that year
            year = first_date_row['data'].year
            january_1st = pd.Timestamp(year=year, month=1, day=1)

            # If your system uses 1=Monday, 7=Sunday, add 1:
            start_weekday = january_1st.weekday() + 1
```
**Categorização detalhada**:
- **non_holidays**: dias do ano que não são feriados fechados
- **start_weekday**: dia da semana (1 a 7 onde 1 é segunda) do primeiro dia do ano



### 4.4 Construção da Estrutura Semanal
```python
week_to_days = {}
week_to_days_salsa = {}
            
# Process each unique date in the calendar (remove duplicates by date)
unique_calendar_dates = matriz_calendario_gd.drop_duplicates(['data']).sort_values('data')

for _, row in unique_calendar_dates.iterrows():
    day_of_year = row['data'].dayofyear
    week_number = row['ww']  # Use WW column for week number
    
    # Initialize the week list if it doesn't exist
    if week_number not in week_to_days:
        week_to_days[week_number] = []
    
    if week_number not in week_to_days_salsa:
        week_to_days_salsa[week_number] = []
    
    if day_of_year not in week_to_days_salsa[week_number]:
        week_to_days_salsa[week_number].append(day_of_year)
    # Add the day to its corresponding week (avoid duplicates)
    if day_of_year not in week_to_days[week_number] and day_of_year in non_holidays:
        week_to_days[week_number].append(day_of_year)
```

**Estruturas criadas**:
- **week_to_days_salsa**: mapeamento completo semana → todos os dias
- **week_to_days**: mapeamento semana sem dias de feriado fechado


## 5. Processamento Detalhado por Colaborador

### 5.1 Categorização Detalhada de Dias por Colaborador

Para cada colaborador, são extraídas listas de dias por categoria, com base no campo `tipo_turno` do calendário. Além disso, dias em que o colaborador não aparece no calendário são adicionados a `empty_days`.

```python
worker_calendar = matriz_calendario_gd[matriz_calendario_gd['colaborador'] == w]
worker_present_days = set(worker_calendar['data'].dt.dayofyear.tolist())
days_not_in_calendar = set(days_of_year) - worker_present_days

worker_empty = worker_calendar[worker_calendar['tipo_turno'] == '-']['data'].dt.dayofyear.tolist()
worker_missing = worker_calendar[worker_calendar['tipo_turno'] == 'V']['data'].dt.dayofyear.tolist()
w_holiday = worker_calendar[(worker_calendar['tipo_turno'] == 'A') | (worker_calendar['tipo_turno'] == 'AP')]['data'].dt.dayofyear.tolist()
worker_fixed_days_off = worker_calendar[worker_calendar['tipo_turno'] == 'L']['data'].dt.dayofyear.tolist()
f_day_complete_cycle = worker_calendar[worker_calendar['tipo_turno'].isin(['L', 'L_DOM'])]['data'].dt.dayofyear.tolist()
```

**Significado de cada categoria**:
- **empty_days**: dias onde o colaborador não está disponível ou não aparece no calendário
- **missing_days**: dias de ausência/vazio
- **worker_holiday**: ausências justificadas
- **fixed_days_off**: folgas já pré-atribuídas
- **fixed_LQs**: fins de semana de qualidade já determinados através das 5 ausencias numa semana
- **free_day_complete_cycle**: folgas para trabalhadores de ciclo completo

### 5.2 Extração de Datas Contratuais

Para cada colaborador, são extraídas as datas de admissão e demissão, convertidas para o formato `dayofyear` e validadas contra o intervalo do calendário. Se a data estiver fora do intervalo, é ignorada (definida como 0).

```python
for w in workers_complete:
    worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
    admissao_value = worker_data.iloc[0].get('data_admissao', None) if not worker_data.empty else None
    demissao_value = worker_data.iloc[0].get('data_demissao', None) if not worker_data.empty else None

    # Admissão
    if admissao_value is not None and not pd.isna(admissao_value):
        if isinstance(admissao_value, (datetime, pd.Timestamp)):
            admissao_date = admissao_value
        elif isinstance(admissao_value, str):
            admissao_date = pd.to_datetime(admissao_value)
        else:
            admissao_date = None
        if admissao_date is not None and min_calendar_date <= admissao_date <= max_calendar_date:
            data_admissao[w] = int(admissao_date.dayofyear)
        else:
            data_admissao[w] = 0
    else:
        data_admissao[w] = 0

    # Demissão
    if demissao_value is not None and not pd.isna(demissao_value):
        if isinstance(demissao_value, (datetime, pd.Timestamp)):
            demissao_date = demissao_value
        elif isinstance(demissao_value, str):
            demissao_date = pd.to_datetime(demissao_value)
        else:
            demissao_date = None
        if demissao_date is not None and min_calendar_date <= demissao_date <= max_calendar_date:
            data_demissao[w] = int(demissao_date.dayofyear)
        else:
            data_demissao[w] = 0
    else:
        data_demissao[w] = 0
```


### 5.3 Rastreamento de Primeiro e Último Dia

O primeiro e último dia registado são calculados a partir do calendário, ajustados pelas datas de admissão/demissão se necessário.

```python
if w in matriz_calendario_gd['colaborador'].values:
                first_registered_day[w] = worker_calendar['data'].dt.dayofyear.min()
                if first_registered_day[w] < data_admissao[w]:
                    first_registered_day[w] = data_admissao[w]
                logger.info(f"Worker {w} first registered day: {first_registered_day[w]}")
            else:
                first_registered_day[w] = 0

            if w in matriz_calendario_gd['colaborador'].values:
                last_registered_day[w] = worker_calendar['data'].dt.dayofyear.max()
                # Only adjust if there's an actual dismissal date (not 0)
                if data_demissao[w] > 0 and last_registered_day[w] > data_demissao[w]:
                    last_registered_day[w] = data_demissao[w]
                logger.info(f"Worker {w} last registered day: {last_registered_day[w]}")
            else:
                last_registered_day[w] = 0
```

### 5.4 Tratamento de Ausências e Limpeza

Dias antes da admissão e após a demissão são marcados como `missing_days`. Todas as categorias são limpas de feriados encerrados e fixed_days_off e fixed_LQs são calculados.

```python
if first_registered_day[w] > 0 or last_registered_day[w] > 0:
    missing_days[w].extend([d for d in range(1, first_registered_day[w]) if d not in missing_days[w]])
    missing_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in missing_days[w]])

empty_days[w] = sorted(list(set(empty_days[w]) - set(closed_holidays)))
fixed_days_off[w] = sorted(list(set(fixed_days_off[w]) - set(closed_holidays)))
worker_holiday[w], fixed_days_off[w], fixed_LQs[w] = data_treatment(
    set(worker_holiday[w]) - set(closed_holidays) - set(fixed_days_off[w]),
    set(fixed_days_off[w]),
    week_to_days_salsa,
    start_weekday,
    set(closed_holidays)
)
missing_days[w] = sorted(list(set(missing_days[w]) - set(closed_holidays)))
free_day_complete_cycle[w] = sorted(list(set(free_day_complete_cycle[w]) - set(closed_holidays)))
```

### 5.5 Cálculo Final de Dias de Trabalho

Dias de trabalho são calculados por subtração de todas as categorias de indisponibilidade.

```python
working_days[w] = set(days_of_year) - set(empty_days[w]) - set(worker_holiday[w]) - set(missing_days[w]) - set(closed_holidays)
```

**Lógica de subtração**:
- **Base**: todos os dias do período (`days_of_year`)
- **Subtraem-se**:
  - `empty_days[w]`: dias indisponíveis
  - `worker_holiday[w]`: ausências justificadas
  - `missing_days[w]`: dias vazios/faltas
  - `closed_holidays`: encerramentos gerais
- **Resultado**: dias onde o colaborador pode trabalhar ou ter folga atribuída

### 5.6 Validação Final

Se não houver dias válidos para o colaborador, é gerado um warning.

```python
if not working_days[w]:
    logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")
```

---

## 6. Extração de Informação Contratual

A extração da informação contratual é crítica para o algoritmo, pois define os parâmetros que governam as restrições de cada colaborador. Este processo valida e extrai todos os elementos contratuais necessários.

### 6.1 Dicionários de Parâmetros Contratuais
```python
contract_type = {}
total_l = {}
total_l_dom = {}
c2d = {}
c3d = {}
l_d = {}
l_q = {}
cxx = {}
t_lq = {}
```

### 6.2 Extração e Validação Detalhada
```python
for w in workers:
    worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
    
    if worker_data.empty:
        logger.warning(f"No contract data found for worker {w}")
        # Set default values
        contract_type[w] = 'Contract Error'
        total_l[w] = 0
        total_l_dom[w] = 0
        c2d[w] = 0
        c3d[w] = 0
        l_d[w] = 0
        l_q[w] = 0
        cxx[w] = 0
    else:
        worker_row = worker_data.iloc[0]
        contract_type[w] = worker_row.get('tipo_contrato', 'Contract Error')
        total_l[w] = int(worker_row.get('l_total', 0))
        total_l_dom[w] = int(worker_row.get('l_dom_salsa', 0))
        c2d[w] = int(worker_row.get('c2d', 0))
        c3d[w] = int(worker_row.get('c3d', 0))
        l_d[w] = int(worker_row.get('l_d', 0))
        l_q[w] = int(worker_row.get('l_q', 0))
        cxx[w] = int(worker_row.get('cxx', 0))
        t_lq[w] = int(worker_row.get('lqs', 0))
```

### 6.3 Validação e Limpeza de Colaboradores Inválidos
```python
for w in workers:
    if contract_type[w] == 'Contract Error' or total_l[w] <= 0:
        logger.warning(f"Removing worker {w} due to invalid contract data")
        workers.pop(workers.index(w))
```

**Parâmetros contratuais extraídos**:
- **contract_type**: tipo de contrato (define limites semanais de trabalho)
- **total_l**: total de folgas a atribuir no período (alcampo)
- **total_l_dom**: folgas mínimas em domingos 
- **c2d**: fins de semana de qualidade mínimos (sábado + domingo de folga)
- **c3d**: fins de semana estendidos (sábado + domingo + segunda/sexta)
- **l_d**: folgas de compensação por trabalho em domingos
- **l_q**: folgas livres (calculadas anteriormente)
- **cxx**: limite máximo de folgas consecutivas por ano
- **t_lq**: total de LQs (fins de semana de qualidade) disponíveis

### 6.4 Definição de Papéis Operacionais

Esta informação é utilizada para atribuição de folgas exclusivas entre managers e keyholders.

```python
role_by_worker = {}
managers = []
keyholders = []

role_col = "prioridade_folgas"  # Coluna que define hierarquia operacional

for w in workers_complete:
    worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
    if not worker_data.empty:
        role_value = worker_data.iloc[0].get(role_col, None)
        
        if role_value == 1:
            role_by_worker[w] = "manager"
            managers.append(w)
        elif role_value == 2:
            role_by_worker[w] = "keyholder"
            keyholders.append(w)
        else:
            role_by_worker[w] = "normal"
    else:
        role_by_worker[w] = "normal"
```

**Hierarquia operacional**:
- **managers** (1): maior prioridade na atribuição de folgas exclusivas
- **keyholders** (2): prioridade intermédia
- **normal**: colaboradores regulares

### 6.5 Ajuste Proporcional para Admissões/Demissões
```python
 proportion = {}
        for w in workers:
            logger.info(f"Adjusting parameters for worker {w} with first registered day {first_registered_day[w]} and last registered day {last_registered_day[w]}")
            if (last_registered_day[w] > 0 and last_registered_day[w] < 364):
                proportion[w] = (last_registered_day[w]- first_registered_day[w])  / (days_of_year[-1] - first_registered_day[w])
                logger.info(f"Adjusting worker {w} parameters based on last registered day {last_registered_day[w]} with proportion[w] {proportion[w]:.2f}")
                total_l[w] = int(round(proportion[w] * total_l[w]))
                total_l_dom[w] = int(round(proportion[w] * total_l_dom[w]))
                c2d[w] = int(math.floor(proportion[w] * c2d[w]))
                c3d[w] = int(math.floor(proportion[w] * c3d[w]))
                l_d[w] = int(round(proportion[w] * l_d[w]))
                l_q[w] = int(round(proportion[w] * l_q[w]))
                cxx[w] = int(round(proportion[w] * cxx[w]))
                t_lq[w] = int(round(proportion[w] * t_lq[w]))
                
                logger.info(f"Worker {w} parameters adjusted for last registered day {last_registered_day[w]}: "
                            f"Total L: {total_l[w]}, "
                            f"Total L DOM: {total_l_dom[w]}, "
                            f"C2D: {c2d[w]}, "
                            f"C3D: {c3d[w]}, "
                            f"L_D: {l_d[w]}, "
                            f"L_Q: {l_q[w]}, "
                            f"CXX: {cxx[w]}, "
                            f"T_LQ: {t_lq[w]}, ")
```

**Lógica de ajuste**:
- Colaboradores admitidos/demitidos a meio do ano ou então que não estão presentes durante o período total têm parâmetros ajustados. Tendo em conta o numero total de dias trabalhados a dividir pelo período entre o último dia do ano e o primeido dia registado do colaborador. Este cálculo é realizado desta forma, pois já é realizado um calcúlo prévio para determinar o número de folgas para atribuir para o resto do ano caso o colabordor fique até ao final mesmo que entre a meio, mas não tem em conta se este sair antes do final do ano. 
- Mantém justiça proporcional nos direitos contratuais

---

## 7. Informação relativa às Estimativas

O processamento das estimativas transforma os dados de necessidades de staffing em parâmetros utilizáveis pelo modelo de otimização, criando alvos e limites para cada dia e turno.

### 7.1 Estruturas de Dados para Optimização
```python
pess_obj = {}
min_workers = {}
max_workers = {}
working_shift = ["M", "T"]
# If estimativas has specific data, process it
if not matriz_estimativas_gd.empty:
    
    for d in days_of_year:
        
        # Process pess_obj for working_shift
        for s in working_shift:
            day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == s)]
            if not day_shift_data.empty:
                # Convert float to integer for OR-Tools compatibility
                pess_obj[(d, s)] = int(round(day_shift_data['pess_obj'].values[0]))
            else:
                pess_obj[(d, s)] = 0  # or any default value you prefer
        
        # Process min/max workers for all shifts
        for shift_type in shifts:
            day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == shift_type)]
            if not day_shift_data.empty:
                            # Convert floats to integers for OR-Tools compatibility
                            min_workers[(d, shift_type)] = int(round(day_shift_data['min_turno'].values[0]))
                            max_workers[(d, shift_type)] = int(round(day_shift_data['max_turno'].values[0]))
```

**Estruturas criadas**:
- **pess_obj[(d, turno)]**: objetivo de pessoas para o dia d no turno específico
- **min_workers[d, shift_type]**: mínimo de trabalhadores para o dia d e turno shift_type
- **max_workers[d, shift_type]**: máximo de trabalhadores para o dia d e turno shift_type

**Impacto na otimização**:
- `pess_obj` define alvos específicos para a função objetivo
- `min_workers` cria restrições soft de staffing mínimo
- `max_workers` previne sobre-staffing excessivo

---

## 8. Turnos durante a Semana

O mapeamento de turnos semanais define as restrições de turnos para cada colaborador em cada semana.

### 8.1 Estrutura do Mapeamento Semanal
```python
worker_week_shift = {}

# Iterate over each worker
for w in workers_complete:
    # Only iterate over weeks that actually exist in week_to_days
    for week in week_to_days.keys():  # Use only existing weeks instead of range(1, 53)
        worker_week_shift[(w, week, 'M')] = 0
        worker_week_shift[(w, week, 'T')] = 0
        
        # Iterate through days of the week for the current week
        for day in week_to_days[week]:
                
                # Get the rows for the current week and day
                 # Use WW column instead of isocalendar().week for consistency
                shift_entries = matriz_calendario_gd[
                    (matriz_calendario_gd['ww'] == week) & 
                    (matriz_calendario_gd['data'].dt.day_of_year == day) & 
                    (matriz_calendario_gd['colaborador'] == w)
                ]
                
                #logger.info(f"Processing worker {w}, week {week}, day {day}: found {len(shift_entries)} shift entries with types: {shift_entries['tipo_turno'].tolist() if not shift_entries.empty else 'None'}")
                # Check for morning shifts ('M') for the current worker
                if not shift_entries[shift_entries['tipo_turno'] == "M"].empty:
                    # Assign morning shift to the worker for that week
                    worker_week_shift[(w, week, 'M')] = 1  # Set to 1 if morning shift is found
                # Check for afternoon shifts ('T') for the current worker
                if not shift_entries[shift_entries['tipo_turno'] == "T"].empty:
                    # Assign afternoon shift to the worker for that week
                    worker_week_shift[(w, week, 'T')] = 1  # Set to 1 if afternoon shift is found  # (worker, week, shift) -> valor preferencial
```
**Estruturas criadas**:
- **worker_week_shift[(w, week, shift_type)]**: valor 1 se o trabalhador 'w', pode trabalhar na semana 'week', o turno 'shift_type'. 0 se não puder

---

## 9. Função data_treatment()

A função `data_treatment()` é responsável pelo processamento avançado de dias de folga quando no resto da semana a pessoa está ausente

### 9.1 Assinatura e Parâmetros
```python
def data_treatment(worker_holiday, fixed_days_off, week_to_days_salsa, start_weekday, closed_holidays):
    """
    Processa e categoriza dias especiais para um colaborador.
    
    Args:
        worker_holiday: set de dias de ausência justificada
        fixed_days_off: set de folgas já pré-atribuídas
        week_to_days_salsa: mapeamento semana -> dias
        start_weekday: dia da semana do primeiro dia do ano
        closed_holidays: set de feriados de encerramento
        
    Returns:
        worker_holiday_processed: ausências processadas
        fixed_days_off_processed: folgas processadas
        fixed_LQs: fins de semana de qualidade identificados
    """
```

### 9.2 Identificação de Fins de Semana de Qualidade
Se uma semana tiver mais de 5 ou mais dias de Ausência, os dois dias mais avançados são considerados folgas invés de ausênsias. Se estes foram sábado e domingo, transforma as auências num fim de semana de folga, se não, são folgas normais

```python
fixed_LQs = []
for week, days in week_to_days_salsa.items():
    if (len(days) <= 6):
        continue
    
    days_set = set(days)
    holiday_days_in_week = days_set.intersection(worker_holiday)
    if len(list(holiday_days_in_week)) >= 5:
        atributing_days = list(sorted(days_set - closed_holidays))
        l1 = atributing_days[-1]
        l2 = atributing_days[-2]
        if l1 == days[6] and l2 == days[5]:
            worker_holiday -= {l2, l1}
            fixed_days_off |= {l1}
            fixed_LQs.append(l2)
        else:
            worker_holiday -= {l2,l1}
            fixed_days_off |= {l2,l1}
```
**Estruturas criadas**:
- **worker_holiday**: são retirados dois dias de auência ao


**Transformações realizadas**:
- **Identificação de LQs**: sábados que fazem parte de fins de semana de qualidade
- **Remoção de sobreposições**: elimina feriados encerrados de todas as categorias
- **Categorização precisa**: separa folgas normais (L) de folgas de qualidade (LQ)
- **Validação temporal**: confirma que os fins de semana estão corretos

### 9.5 Retorno Estruturado
```python
return worker_holiday_processed, fixed_days_off_processed, fixed_LQs
```

**Importância da função**:
- Garante que folgas de qualidade (LQ) são identificadas corretamente
- Evita conflitos entre diferentes tipos de ausências
- Prepara dados para restrições específicas do algoritmo SALSA
- Mantém consistência temporal e lógica nos dados

---

## 10. Retorno dos Dados

A função `read_data_salsa()` retorna uma tupla extensa contendo todos os dados processados e estruturas necessárias para o algoritmo de otimização. Esta secção detalha cada elemento retornado.

### 10.1 Estrutura de Retorno
```python
return (
    matriz_calendario_gd, days_of_year, sundays, holidays, special_days, closed_holidays,
    empty_days, worker_holiday, missing_days, working_days, non_holidays, start_weekday,
    week_to_days, worker_week_shift, matriz_colaborador_gd, workers, contract_type,
    total_l, total_l_dom, c2d, c3d, l_d, l_q, cxx, t_lq, matriz_estimativas_gd,
    pess_obj, min_workers, max_workers, workers_complete, workers_complete_cycle,
    free_day_complete_cycle, week_to_days_salsa, first_day, admissao_proporcional,
    data_admissao, data_demissao, last_day, fixed_days_off, fixed_LQs, role_by_worker,
    proportion
)
```

### 10.2 Descrição Detalhada dos Elementos Retornados

#### **Dados Base Processados**
1. **`matriz_calendario_gd`**: DataFrame do calendário filtrado para colaboradores válidos
2. **`matriz_colaborador_gd`**: DataFrame dos colaboradores filtrado para colaboradores válidos
3. **`matriz_estimativas_gd`**: DataFrame das estimativas (inalterado)

#### **Informação Temporal**
4. **`days_of_year`**: Lista ordenada de dias do ano que estão presentes na matriz calendario (formato dayofyear: 1-365/366)
5. **`start_weekday`**: Dia da semana do primeiro dia do ano (1=segunda, 7=domingo)
6. **`week_to_days`**: Dicionário {semana: [dias]} excluindo feriados encerrados
7. **`week_to_days_salsa`**: Dicionário {semana: [dias]} incluindo todos os dias

#### **Categorização de Dias Especiais**
8. **`sundays`**: Lista de domingos (dayofyear)
9. **`holidays`**: Lista de feriados que não são domingos (dayofyear)
10. **`special_days`**: Lista combinada de domingos + feriados (dayofyear)
11. **`closed_holidays`**: Lista de dias de encerramento forçado (dayofyear)
12. **`non_holidays`**: Lista de dias que não são feriados encerrados (dayofyear)

#### **Listas de Colaboradores**
13. **`workers`**: Lista de colaboradores que necessitam atribuição algorítmica (ciclo != 'Completo')
14. **`workers_complete`**: Lista de todos os colaboradores válidos
15. **`workers_complete_cycle`**: Lista de colaboradores de ciclo completo (folgas pré-definidas)

#### **Dados por Colaborador - Disponibilidade**
16. **`empty_days`**: Dict {worker: [dias]} onde colaborador não está disponível
17. **`worker_holiday`**: Dict {worker: [dias]} de ausências justificadas processadas
18. **`missing_days`**: Dict {worker: [dias]} de ausências/vazios
19. **`working_days`**: Dict {worker: set(dias)} onde colaborador pode trabalhar ou ter folga
20. **`fixed_days_off`**: Dict {worker: [dias]} de folgas já pré-atribuídas
21. **`fixed_LQs`**: Dict {worker: [dias]} sábados de folga de fins de semana de qualidade identificados
22. **`free_day_complete_cycle`**: Dict {worker: [dias]} de folgas para colaboradores de ciclo completo

#### **Dados por Colaborador - Contratuais**
23. **`contract_type`**: Dict {worker: tipo_contrato} define limites semanais de dias de trabalho
24. **`total_l`**: Dict {worker: int} total de folgas a atribuir (ajustado proporcionalmente) (alcampo)
25. **`total_l_dom`**: Dict {worker: int} folgas mínimas em domingos/especiais
26. **`c2d`**: Dict {worker: int} fins de semana de folga mínimos
27. **`c3d`**: Dict {worker: int} fins de semana de 3 dias (alcampo)
28. **`l_d`**: Dict {worker: int} folgas de compensação por trabalho em domingos (alcampo)
29. **`l_q`**: Dict {worker: int} folgas livres calculadas (alcampo)
30. **`cxx`**: Dict {worker: int} limite máximo de folgas consecutivas (alcampo)
31. **`t_lq`**: Dict {worker: int} total de LQs disponíveis (alcampo)

#### **Dados por Colaborador - Temporais**
32. **`data_admissao`**: Dict {worker: dayofyear} data de admissão (0 se fora do período)
33. **`data_demissao`**: Dict {worker: dayofyear} data de demissão (0 se fora do período)
34. **`first_registered_day`**: Dict {worker: dayofyear} primeiro dia registado (renomeado de first_registered_day)
35. **`last_registered_day`**: Dict {worker: dayofyear} último dia registado (renomeado de last_registered_day)
36. **`proportion`**: Dict {worker: float} proporção de dias trabalhados vs período completo

#### **Dados por Colaborador - Operacionais**
37. **`role_by_worker`**: Dict {worker: role} hierarquia operacional ('manager'/'keyholder'/'normal')
38. **`worker_week_shift`**: Dict {(worker, week, shift): float} possibilidade de trabalhar X turno na semana Y

#### **Estimativas e Objetivos**
39. **`pess_obj`**: Dict {(day, turno): int} objetivos de staffing por dia e turno
40. **`min_workers`**: Dict {day: int} mínimo absoluto de trabalhadores por dia
41. **`max_workers`**: Dict {day: int} máximo de trabalhadores por dia

#### **Parâmetros de Configuração**
42. **`admissao_proporcional`**: String que determina se o arredondamento na semana de admissão e demissão é feito para baixo (floor) ou para cima (ceil)

### 10.3 Validação Final dos Dados
```python
logger.info("[OK] Data processing completed successfully")
logger.info(f"[RETURN SUMMARY]:")
logger.info(f"  - Total elements returned: 42")
logger.info(f"  - Workers for algorithm: {len(workers)}")
logger.info(f"  - Total workers: {len(workers_complete)}")
logger.info(f"  - Working days span: {min(days_of_year)} to {max(days_of_year)}")
logger.info(f"  - Special days: {len(special_days)}")
logger.info(f"  - Optimization targets: {len(pess_obj)}")
```

**Características dos dados retornados**:
- **Completude**: todos os elementos necessários para o modelo CP-SAT
- **Consistência**: dados validados e filtrados para colaboradores válidos
- **Estruturação**: organizados por tipo e uso no algoritmo
- **Rastreabilidade**: mantém ligação aos dados originais via IDs
- **Otimização**: estruturas preparadas para performance no solver

Estes dados formam a base completa para a construção do modelo de programação por restrições, permitindo que o algoritmo SALSA execute com todas as informações necessárias para produzir horários optimizados e válidos.
---

# Criação das Variáveis

O modelo de programação por restrições utiliza um sistema de variáveis booleanas para representar todas as decisões de atribuição de turnos. Esta secção detalha cada aspecto da criação e estruturação dessas variáveis.

## 1. Arquitectura da Variável Principal: `shift`

### 1.1 Estrutura Tridimensional
```python
shift[(worker, day, shift_type)] → cp_model.BoolVar
```

**Dimensões explicadas**:
- **Dimensão 1 (worker)**: identificador numérico do colaborador (ex: 80001676)
- **Dimensão 2 (day)**: dia do ano (1-365/366) 
- **Dimensão 3 (shift_type)**: tipo de turno/estado ("M", "T", "L", "LQ", "F", "V", "A", "-")

**Domínio matemático**: {0, 1} (variável booleana integral)

**Interpretação semântica**:
- `shift[(80001676, 125, "M")] = 1`: colaborador 80001676 trabalha turno manhã no dia 125
- `shift[(80001676, 125, "M")] = 0`: colaborador 80001676 NÃO trabalha turno manhã no dia 125

### 1.2 Criação Algorítmica das Variáveis
```python
def add_var(model, shift, w, days, code, start_weekday):
    for d in days:
        if (code == 'L' and (d + start_weekday - 2) % 7 == 5 and d + 1 in days):
            shift[(w, d, 'LQ')] = model.NewBoolVar(f"{w}_Day{d}_'LQ'")
            model.Add(shift[(w, d, 'LQ')] == 1)
        else:
            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
            model.Add(shift[(w, d, code)] == 1)


def decision_variables(model, days_of_year, workers, shifts, first_day, last_day, absences, missing_days, empty_days, closed_holidays, fixed_days_off, fixed_LQs, start_weekday):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')
    shifts2.remove('-')
 
    closed_set = set(closed_holidays)
    for w in workers:
 
        
        empty_days_set = set(empty_days[w])
        missing_set = (set(missing_days[w]) | empty_days_set) - closed_set
        fixed_LQs_set = set(fixed_LQs[w])- missing_set - closed_set
        fixed_days_set = set(fixed_days_off[w]) - missing_set - closed_set - fixed_LQs_set
        absence_set = set(absences[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set
        logger.info(f"DEBUG worker {w}")
        logger.info(f"DEBUG empty days {empty_days_set}")
        logger.info(f"DEBUG missing {missing_set}")
        logger.info(f"DEBUG fixed days {fixed_days_set}")
        logger.info(f"DEBUG fixed lqs {fixed_LQs_set}")
        logger.info(f"DEBUG absence {absence_set}")
 
        blocked_days = absence_set | missing_set | empty_days_set | closed_set | fixed_days_set | fixed_LQs_set

 
        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")

        add_var(model, shift, w, missing_set - absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'V', start_weekday)
        add_var(model, shift, w, absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'A', start_weekday)
        add_var(model, shift, w, fixed_days_set - closed_set - fixed_LQs_set - empty_days_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set - closed_set - empty_days_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_set - empty_days_set, 'F', start_weekday)
        add_var(model, shift, w, empty_days_set, '-', start_weekday)
```

**Processo de filtragem**:
1. **Filtragem temporal**: só cria variáveis entre first_day[w] e last_day[w]
2. **Filtragem por exclusões**: atribue automaticamente dias já pré-determinados e remove as outras opções das variáveis
3. **Filtragem por compatibilidade**: verifica se turno é válido para o dia da semana
4. **Nomeação sistemática**: cada variável tem nome único para debug

### 1.3 Tipos de Turnos e Suas Implicações

#### Turnos de Trabalho
- **"M" (Manhã)**: 
  - Compatível com: dias úteis, alguns fins de semana
  - Incompatível com: feriados encerrados
  
- **"T" (Tarde)**: 
  - Compatível com: dias úteis, alguns fins de semana
  - Incompatível com: feriados encerrados

#### Estados de Não-Trabalho
- **"L" (Folga normal)**: 
  - Dia de descanso regular
  - Conta para quotas de folgas semanais
  - Pode ser consecutivo (com limites)
  
- **"LQ" (Folga de qualidade)**: 
  - Folga no sábado em fim de semana de qualidade
  - Sujeita a regras especiais (sábado LQ + domingo L)
  - Limitada por quotas contratuais (c2d)
  
- **"F" (Feriado encerrado)**: 
  - Dia de encerramento geral
  - Atribuição forçada (não optimizável)
  - Remove-se de todas as outras categorias

#### Estados de Ausência
- **"V" (Vazio)**: 
  - Colaborador não disponível
  - Dias antes da admissão ou após demissão
  - Dias marcados como indisponíveis no calendário
  
- **"A" (Ausência)**: 
  - Falta justificada ou injustificada
  - Pré-determinada no calendário histórico
  - Não optimizável pelo algoritmo

- **"-" (Não atribuído)**: 
  - Estado temporário durante optimização
  - Indica falha na atribuição
  - Deve ser minimizado na solução final

## 2. Restrições na Criação de Variáveis


### 2.1 Exclusão de Dias Pré-determinados
```python
add_var(model, shift, w, missing_set - absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'V', start_weekday)
        add_var(model, shift, w, absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'A', start_weekday)
        add_var(model, shift, w, fixed_days_set - closed_set - fixed_LQs_set - empty_days_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set - closed_set - empty_days_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_set - empty_days_set, 'F', start_weekday)
        add_var(model, shift, w, empty_days_set, '-', start_weekday)
```

**Impacto na optimização**:
- **Redução do espaço de busca**: menos variáveis = menos tempo de resolução
- **Garantia de consistência**: pré-atribuições são respeitadas automaticamente
- **Prevenção de conflitos**: evita situações logicamente impossíveis

### 2.2 Validação de Compatibilidade Turno-Dia
Só permite atribuição de turnos LQ se for 1 sábado

### 2.3 Considerações de Memória e Performance
**Estimativa de variáveis**:
```
Total variáveis ≈ workers × days × shifts × eligibility_rate
Exemplo: 50 workers × 365 days × 8 shifts × 0.7 = ~102,200 variáveis
```

**Optimizações implementadas**:
- **Lazy creation**: só cria variáveis quando necessário
- **Sparse representation**: usa dicionários em vez de arrays
- **Naming convention**: nomes sistemáticos para debug eficiente

---

# Restrições

O sistema de restrições é o coração do modelo SALSA, garantindo que todas as soluções encontradas respeitam as regras contratuais, operacionais e de bem-estar. Cada restrição é implementada como um conjunto de desigualdades lineares que o solver CP-SAT pode processar eficientemente.

## 1. Restrições Fundamentais (Consistência Básica)

### 1.1 `shift_day_constraint()` - Unicidade de Atribuição Diária
```python
def shift_day_constraint(model, shift, days_of_year, workers_complete, shifts):
    for w in workers_complete:
        for d in days_of_year:
            total_shifts = []
            for s in shifts:
                if (w, d, s) in shift:
                    total_shifts.append(shift[(w, d, s)])
            
            if total_shifts:
                model.add_exactly_one(total_shifts)
```

**Matemática subjacente**:
```
∀ worker w, ∀ day d: Σ(shift[(w,d,s)]) = 1 para todos os shifts s válidos
```

**Análise detalhada**:
- **Objectivo**: garantir que cada colaborador tem exactamente um estado por dia
- **Prevenção de conflitos**: impede situações como "manhã E tarde no mesmo dia"
- **Tratamento de ausências**: inclui estados V, A, F como alternativas válidas
- **Condição de eligibilidade**: só aplica se existem variáveis para (w,d)

**Impacto computacional**:
- **Propagação forte**: cada atribuição elimina imediatamente todas as outras opções
- **Detecção precoce de conflitos**: falhas são identificadas rapidamente
- **Redução do espaço de busca**: força decisões binárias limpas

**Exemplo de instanciação**:
```
Colaborador 80001676, Dia 125:
shift[(80001676, 125, "M")] + shift[(80001676, 125, "T")] + 
shift[(80001676, 125, "L")] + shift[(80001676, 125, "LQ")] = 1
```

### 1.2 `week_working_days_constraint()` - Limite Contratual Semanal

Trabalhador, numa semana, só pode trabalhar o número de dias definido no contrato

```python
def week_working_days_constraint(model, shift, week_to_days, workers, working_shift, contract_type):
    for w in workers:
        for week in week_to_days.keys():
            days_in_week = week_to_days[week]
            total_shifts = sum(shift[(w, d, s)] 
                             for d in days_in_week 
                             for s in working_shift 
                             if (w, d, s) in shift)
            max_days = contract_type.get(w, 0)
            model.Add(total_shifts <= max_days)
```

**Formulação matemática**:
```
∀ worker w, ∀ week: Σ(shift[(w,d,"M")] + shift[(w,d,"T")]) ≤ contract_type[w]
onde d ∈ days_of_week
```

**Tipos contratuais comuns**:
- **Contrato tipo 4**: máximo 4 dias de trabalho por semana
- **Contrato tipo 5**: máximo 5 dias de trabalho por semana  
- **Contrato tipo 6**: máximo 6 dias de trabalho por semana

**Análise do impacto**:
- **Compliance legal**: garante respeito pelos limites contratuais
- **Flexibilidade de distribuição**: permite diferentes padrões semanais
- **Interacção com folgas**: mais dias de trabalho = menos folgas disponíveis

**Exemplo numérico**:
```
Colaborador com contrato tipo 4, Semana 15:
Σ(turnos M e T nos dias 99-105) ≤ 4
Se trabalha seg, ter, qua, qui: 4 ≤ 4 ✓
Se trabalha seg, ter, qua, qui, sex: 5 ≤ 4 ✗
```

### 1.3 `maximum_continuous_working_days()` - Limite de Dias Consecutivos

Trabalhador não pode trabalhar mais do que o número máximo de dias de trabalho consecutivamente.

```python
def maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, maxi):
    for w in workers:
        for d in range(1, max(days_of_year) - maxi + 1):
            consecutive_days = sum(
                shift[(w, d + i, s)] 
                for i in range(maxi + 1)
                for s in working_shift
                if (w, d + i, s) in shift
            )
            model.Add(consecutive_days <= maxi)
```

**Formulação matemática**:
```
∀ worker w, ∀ janela de (maxi + 1) dias consecutivos:
Σ(turnos de trabalho) ≤ maxi
```

**Lógica da janela deslizante**:
- **Tamanho da janela**: `maxi + 1` dias
- **Máximo permitido**: `maxi` dias de trabalho na janela
- **Sliding window**: a janela move-se dia a dia pelo calendário
- **Overlap detection**: detecta violações em qualquer posição

**Exemplo com maxi=6**:
```
Dias 100-106 (janela de 7 dias):
Se trabalha 100,101,102,103,104,105,106: 7 ≤ 6 ✗ (violação)
Se trabalha 100,101,102,103,104,105: 6 ≤ 6 ✓
```

- **Compliance regulamentar**: respeita leis laborais



### 1.4 `LQ_attribution()` - Quota Mínima de Fins de Semana de Qualidade

Número mínimo fins de semana de qualidade tem de ser atribuído.

```python
def LQ_attribution(model, shift, workers, working_days, c2d):
    for w in workers:
        model.Add(sum(shift[(w, d, "LQ")] 
                     for d in working_days[w] 
                     if (w, d, "LQ") in shift) >= c2d.get(w, 0))
```

**Matemática**:
```
∀ worker w: Σ(shift[(w,d,"LQ")]) ≥ c2d[w] para d ∈ working_days[w]
```

**Significado do c2d**:
- **c2d**: "Two-day weekends" - fins de semana de folga contratuais
- **Quota mínima**: garantia contratual de fins de semana livres
- **Flexibilidade de timing**: algoritmo escolhe quando atribuir


### 1.5 `assign_week_shift()` - Padrão Semanal de Turnos

Só permite atribuição dos turnos M e T se nessa semana ele puder trabalhar esses turnos. 

```python
def assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift):
    for w in workers:
        for week in week_to_days.keys():
            for day in week_to_days[week]:
                if day in working_days[w]:
                    # Aplicar padrão definido em worker_week_shift
                    if worker_week_shift.get((w, week)) == "M":
                        # Forçar apenas turnos de manhã esta semana
                        if (w, day, "T") in shift:
                            model.Add(shift[(w, day, "T")] == 0)
                    elif worker_week_shift.get((w, week)) == "T":
                        # Forçar apenas turnos de tarde esta semana
                        if (w, day, "M") in shift:
                            model.Add(shift[(w, day, "M")] == 0)
```

**Objectivo**:
- **Consistência semanal**: colaborador trabalha sempre o turno possível (determinado por worker_week_shift) numa semana



### 1.6 `working_day_shifts()` - Turnos Válidos em Dias de Trabalho

Os dias de trabalho de um colabordor só podem ser ter turnos do tipo: M, T, L e LQ. Se for um trabalhador de ciclo completo, os seus turnos só podem ser do tipo M e T.

```python
def working_day_shifts(model, shift, workers, working_days, check_shift, 
                      workers_complete_cycle, working_shift):
    # Para colaboradores normais
    for w in workers:
        for d in working_days[w]:
            total_shifts = []
            for s in check_shift:  # ['M', 'T', 'L', 'LQ']
                if (w, d, s) in shift:
                    total_shifts.append(shift[(w, d, s)])
            if total_shifts:
                model.add_exactly_one(total_shifts)
    
    # Para colaboradores de ciclo completo
    for w in workers_complete_cycle:
        for d in working_days[w]:
            total_shifts = []
            for s in working_shift:  # ['M', 'T']
                if (w, d, s) in shift:
                    total_shifts.append(shift[(w, d, s)])
            if total_shifts:
                model.add_exactly_one(total_shifts)
```

**Diferenciação por tipo de colaborador**:
- **Colaboradores normais**: podem ter M, T, L, LQ em dias de trabalho
- **Colaboradores de ciclo completo**: apenas M, T (folgas pré-definidas)


### 1.7 `salsa_2_consecutive_free_days()` - Limite de Folgas Consecutivas

Não permite atribuição de 3 dias de folga/feriado seguidas

```python
def salsa_2_consecutive_free_days(model, shift, workers, working_days):
for w in workers: 
        # Get all working days for this worker
        all_work_days = sorted(working_days[w])
        
        # Create boolean variables for each day indicating if it's a free day (L, F, or LQ)
        free_day_vars = {}
        for d in all_work_days:
            free_day = model.NewBoolVar(f"free_day_{w}_{d}")
            
            # Sum the L, F, and LQ shifts for this day
            # If F_special_day is True, consider F shifts as well
            free_shift_sum = sum(
                    shift.get((w, d, shift_type), 0) 
                    for shift_type in ["L", "F", "LQ"]
                )

           
            
            # Link the boolean variable to whether any free shift is assigned
            model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
            model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())
            
            free_day_vars[d] = free_day
        
        # For each consecutive triplet of days in the worker's schedule
        for i in range(len(all_work_days) - 2):
            day1 = all_work_days[i]
            day2 = all_work_days[i+1]
            day3 = all_work_days[i+2]
            
            # Only apply constraint if days are actually consecutive
            if day2 == day1 + 1 and day3 == day2 + 1:
                # At least one of any three consecutive days must NOT be a free day
                model.AddBoolOr([
                    free_day_vars[day1].Not(), 
                    free_day_vars[day2].Not(), 
                    free_day_vars[day3].Not()
                ])
```

**Lógica de prevenção**:
- **Variáveis auxiliares**: `free_day_vars[d]` = 1 se d é folga (L, F, ou LQ)
- **Triplets consecutivos**: verifica janelas de 3 dias seguidos
- **Regra dos 2-em-3**: máximo 2 folgas em qualquer triplet consecutivo


### 1.8 `salsa_2_day_quality_weekend()` - Fins de Semana de Qualidade

Só atribui LQ ao sábado se domingo for L.

```python
def salsa_2_day_quality_weekend(model, shift, workers, contract_type, working_days, 
                               sundays, c2d, F_special_day, days_of_year, closed_holidays):
for w in workers:
    if contract_type[w] in [4, 5, 6]:
        quality_2weekend_vars = []
        
        if F_special_day == False:
            # First, identify all potential 2-day quality weekends (Saturday + Sunday)
            for d in working_days[w]:
                # If this is a Sunday and the previous day (Saturday) is a working day
                if d in sundays and d - 1 in working_days[w]:  
                    # Boolean variables to check if the worker is assigned each shift
                    has_L_on_sunday = model.NewBoolVar(f"has_L_on_sunday_{w}_{d}")
                    has_LQ_on_saturday = model.NewBoolVar(f"has_LQ_on_saturday_{w}_{d-1}")
                    # Connect boolean variables to actual shift assignments
                    model.Add(shift.get((w, d, "L"), 0) >= 1).OnlyEnforceIf(has_L_on_sunday)
                    model.Add(shift.get((w, d, "L"), 0) == 0).OnlyEnforceIf(has_L_on_sunday.Not())
                    model.Add(shift.get((w, d - 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_LQ_on_saturday)
                    model.Add(shift.get((w, d - 1, "LQ"), 0) == 0).OnlyEnforceIf(has_LQ_on_saturday.Not())
                    # Create a binary variable to track whether this weekend qualifies as a 2-day quality weekend
                    quality_weekend_2 = model.NewBoolVar(f"quality_weekend_2_{w}_{d}")
                    # A weekend is "quality 2" only if both conditions are met: LQ on Saturday and L on Sunday
                    model.AddBoolAnd([has_L_on_sunday, has_LQ_on_saturday]).OnlyEnforceIf(quality_weekend_2)
                    model.AddBoolOr([has_L_on_sunday.Not(), has_LQ_on_saturday.Not()]).OnlyEnforceIf(quality_weekend_2.Not())
                    # Track the quality weekend count
                    quality_2weekend_vars.append(quality_weekend_2)
            
            # Constraint: The worker should have at least c2d quality weekends
            model.Add(sum(quality_2weekend_vars) >= c2d.get(w, 0))
            
            # Now ensure LQ shifts ONLY appear on Saturdays before Sundays with L shifts
            # For every working day for this worker
            for d in working_days[w]:
                # If the worker can be assigned an LQ shift on this day
                if (w, d, "LQ") in shift:
                    # This boolean captures if this day could be part of a quality weekend
                    could_be_quality_weekend = model.NewBoolVar(f"could_be_quality_weekend_{w}_{d}")
                    debug_vars[f"could_be_quality_weekend_{w}_{d}"] = could_be_quality_weekend
                    
                    # Conditions for a day to be eligible for LQ:
                    # 1. It must not be a Sunday
                    # 2. The next day must be a Sunday in worker's working days
                    # 3. There must be an L shift on that Sunday
                    
                    eligible_conditions = []
                    
                    # Check if this is a Saturday (day before a Sunday) and the Sunday is a working day
                    if d + 1 in working_days[w] and d + 1 in sundays:
                        # Create a boolean for whether there's a Sunday L shift
                        has_sunday_L = model.NewBoolVar(f"next_day_L_{w}_{d+1}")
                        model.Add(shift.get((w, d + 1, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_L)
                        model.Add(shift.get((w, d + 1, "L"), 0) == 0).OnlyEnforceIf(has_sunday_L.Not())
                        eligible_conditions.append(has_sunday_L)
                    
                    # If no eligible conditions were found, this day can't be part of a quality weekend
                    if eligible_conditions:
                        model.AddBoolAnd(eligible_conditions).OnlyEnforceIf(could_be_quality_weekend)
                        model.AddBoolOr([cond.Not() for cond in eligible_conditions]).OnlyEnforceIf(could_be_quality_weekend.Not())
                    else:
                        model.Add(could_be_quality_weekend == 0)
                    
            #         # Final constraint: LQ can only be assigned if this day could be part of a quality weekend
                    model.Add(shift.get((w, d, "LQ"), 0) <= could_be_quality_weekend)


```

**Definição de fim de semana de qualidade**:
- **Padrão base**: sábado com LQ + domingo com L
- **Variante com F_special_day**: sábado com LQ + domingo com (L ou F)
- **Aplicação restrita**: apenas contratos tipo 4, 5, 6

### 1.9 `salsa_saturday_L_constraint()` - Coordenação Sábado-Domingo

Sábado de folga e domingo de folga exige que sábado seja turno LQ

```python
def salsa_saturday_L_constraint(model, shift, workers, working_days, start_weekday, 
                               days_of_year, non_working_days):

    # For each worker, constrain LQ on Saturday if L on Sunday
    for w in workers:
        for day in working_days[w]:
            # Get day of week (5 = Saturday)
            day_of_week = (day + start_weekday - 2) % 7
            
            # Case 1: Saturday (day_of_week == 5)
            if day_of_week == 5:
                sunday_day = day + 1
                
                # Check if Sunday exists and is within the year bounds
                if sunday_day in working_days[w]:
                    # Check if both Saturday and Sunday shifts exist for this worker
                    saturday_l_key = (w, day, "L")
                    sunday_l_key = (w, sunday_day, "L")
                    
                    if saturday_l_key in shift and sunday_l_key in shift:
                        saturday_l = shift[saturday_l_key]
                        sunday_l = shift[sunday_l_key]
                        #logger.debug(f"DEBUG: Adding constraint for Worker {w}, Saturday {day}, Sunday {sunday_day}")
                        # If Sunday has L, then Saturday can't have L
                        # This translates to: sunday_l == 1 → saturday_l == 0
                        # Which is equivalent to: saturday_l + sunday_l <= 1
                        model.Add(saturday_l + sunday_l <= 1)
```

**Regra de coordenação**:
- **Implicação direccional**: domingo L != sábado L
- **Lógica**: evita padrão "sábado folga normal + domingo folga". Força fim de semana de folga se sábado e domingo forem folgas através da atribuição de L ao sábado

### 1.10 `salsa_2_free_days_week()` - Mínimo de Folgas Semanais

Numa semana, um colabordor só pode ter 2 folgas (contrato de 5 dias, contratos de 4 dias trabalhador tem 3 folgas), a não ser que seja uma semana de admissão e demissão onde o número de folgas deverá ser uma proporção do número de dias trabalhados numa semana arredondado (para baixo no contexto da salsa). Folgas fixas podem ultrapassar este número

```python
for w in workers:
        worker_admissao = data_admissao.get(w, 0)
        worker_demissao = data_demissao.get(w, 0)
        logger.info(f"Worker {w}, Admissao: {worker_admissao}, Demissao: {worker_demissao}, Working Days: {working_days[w]}, Week Days: {week_to_days_salsa}")


        # Create variables for free days (L, F, LQ) by week
        for week, days in week_to_days_salsa.items():
            
            # Only include workdays (excluding weekends)
            week_work_days = [
                d for d in days 
                if d in working_days[w]
            ]
            
            # Sort days to ensure they're in chronological order
            week_work_days.sort()
            # Skip if no working days for this worker in this week
            if not week_work_days:
                continue
            
            week_work_days_set = set(week_work_days)

            fixed_days_week = week_work_days_set.intersection(set(fixed_days_off[w]))
            fixed_lqs_week = week_work_days_set.intersection(set(fixed_LQs[w]))

            # Check if admissao or demissao day falls within this week
            is_admissao_week = (worker_admissao > 0 and worker_admissao in days)
            is_demissao_week = (worker_demissao > 0 and worker_demissao in days)
            
            # If this is an admissao or demissao week, apply proportional calculation
            if is_admissao_week or is_demissao_week:
                # Calculate proportional requirement based on actual days in the week
                # Standard week has 7 days and requires 2 free days
                # Proportion: (actual_days / 7) * 2

                actual_days_in_week = len(week_work_days)  # Total days in this week
                proportion = actual_days_in_week / 7.0
                proportion_days = proportion * 2
                
                # Apply the rounding strategy for admissao/demissao weeks
                if admissao_proporcional == 'floor':
                    required_free_days = max(0, int(floor(proportion_days)))
                    
                elif admissao_proporcional == 'ceil':
                    required_free_days = max(0, int(ceil(proportion_days)))
                    
                else:
                    required_free_days = max(0, int(floor(proportion_days)))

                logger.info(f"Worker {w}, Week {week} (Admissao/Demissao week), Days {week_work_days}: "
                           f"Proportion = {proportion_days:.2f}, Required Free Days = {required_free_days}")
            
            else:
                if len(week_work_days) >= 2:
                    required_free_days = 2
                elif len(week_work_days) == 1:
                    # Partial week with 4+ days: require 1 free day
                    required_free_days = 1
                else:
                     # Very short week: no requirement
                     required_free_days = 0
            
                logger.info(f"Worker {w}, Week {week} (Regular week), Days {week_work_days}: "
                           f"Required Free Days = {required_free_days}")

            if required_free_days < (len(fixed_days_week) + len(fixed_lqs_week)):
                required_free_days = len(fixed_days_week) + len(fixed_lqs_week)
                logger.info(f" Worker {w} - Adjusted Required Free Days to {required_free_days} due to fixed days off: {fixed_days_week}")

            # Only add constraint if we require at least 1 free day

            # Only add constraint if we require at least 1 free day
            if required_free_days >= 0:
                # Create a sum of free shifts for this worker in the current week
                free_shift_sum = sum(
                    shift.get((w, d, shift_type), 0) 
                    for d in week_work_days 
                    for shift_type in ["L", "LQ"]
                )
                


                if required_free_days == 2:
                    if (len(week_work_days) >= 2):
                        model.Add(free_shift_sum == required_free_days)
                elif required_free_days == 1:
                    if (len(week_work_days) >= 1):
                        logger.info(f"Adding constraint for Worker {w}, Week {week}, Required Free Days: {required_free_days}, Free Shift Sum Variable: {free_shift_sum}")
                        model.Add(free_shift_sum == required_free_days)
                elif required_free_days == 0:
                    model.Add(free_shift_sum == 0)
```

**Cálculo proporcional detalhado**:
- **Semana normal**: 2 folgas obrigatórias
- **Semana de admissão**: folgas proporcionais aos dias trabalhados após admissão
- **Semana de demissão**: folgas proporcionais aos dias trabalhados antes da demissão
- **Atribuição de fixos**: folgas já pré-atribuídas reduzem a necessidade

### 1.11 `first_day_not_free()` - Trabalho Obrigatório no Primeiro Dia

Primeiro dia de trabalho de um colaborador não pode ter folga.

```python
def first_day_not_free(model, shift, workers, working_days, first_registered_day, working_shift):
    earliest_first_day = min(first_registered_day.get(w, float('inf')) 
                            for w in workers if first_registered_day.get(w, 0) > 0)
    
    for w in workers:
        worker_first_day = first_registered_day.get(w, 0)
        
        if (worker_first_day > 0 and 
            worker_first_day in working_days[w] and 
            worker_first_day > earliest_first_day):
            
            model.Add(sum(shift.get((w, worker_first_day, shift_type), 0) 
                         for shift_type in working_shift) == 1)
```

**Lógica de aplicação**:
- **Apenas para admissões tardias**: worker_first_day > earliest_first_day
- **Forçar trabalho**: exactamente um turno de trabalho (M ou T)
- **Prevenção de folga imediata**: evita que novos colaboradores tenham logo folga

### 1.12 `free_days_special_days()` - Folgas Mínimas em Domingos

Trabalhador tem de ter o número mínimo de folgas ao domingo

```python
def free_days_special_days(model, shift, sundays, workers, working_days, total_l_dom):
    for w in workers:
        worker_sundays = [d for d in sundays if d in working_days[w]]
        model.Add(sum(shift[(w, d, "L")] for d in worker_sundays) >= total_l_dom.get(w, 0))
```

**Garantia contratual**:
- **total_l_dom[w]**: folgas mínimas obrigatórias em domingos
- **Aplicação específica**: apenas domingos dentro dos working_days
- **Flexibilidade de escolha**: algoritmo selecciona quais domingos

Cada restrição interage com as outras formando um sistema coeso que garante soluções viáveis e optimizadas para todas as partes interessadas.

---

# Otimização

A função de otimização `salsa_optimization()` representa o coração estratégico do algoritmo, definindo como o solver CP-SAT deve equilibrar múltiplos objectivos conflituosos. Esta secção detalha cada componente da função objectivo e como eles interagem para produzir soluções equilibradas.

## 1. Arquitectura da Função Objectivo

### 1.1 Estrutura Matemática Geral
```python
Minimize: Σ(Penalty_Terms) - Σ(Bonus_Terms)
```

**Componentes principais**:
- **Penalty Terms**: penalizações por violações ou situações indesejáveis
- **Bonus Terms**: bonificações por situações favoráveis (peso negativo)
- **Weighted Sum**: cada termo tem peso específico para balancear prioridades

### 1.2 Gestão de Variáveis Auxiliares
```python
    
    objective_terms = []    # Lista final de termos da função objectivo
```

## 2. Objetivos Principais (Operacionais)

### 2.1 Minimização de Desvios das Necessidades (`pess_obj`)

Penaliza desvios do número ideal de pessoas (pess_Obj) para um turno de um dia.
Por cada turno com um trabalhador ou mais ou em falta, a solução é penalizada com 1000 pontos

```python


# Para cada dia e turno
for d in days_of_year:
        for s in working_shift:
            # Calculate the number of assigned workers for this day and shift
            assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
            
            # Create variables to represent the positive and negative deviations from the target
            pos_diff = model.NewIntVar(0, len(workers), f"pos_diff_{d}_{s}")
            neg_diff = model.NewIntVar(0, len(workers), f"neg_diff_{d}_{s}")
            
            # Store the variables in dictionaries
            pos_diff_dict[(d, s)] = pos_diff
            neg_diff_dict[(d, s)] = neg_diff
            
            # Add constraints to ensure that the positive and negative deviations are correctly computed
            model.Add(pos_diff >= assigned_workers - pessObj.get((d, s), 0))  # If excess, pos_diff > 0
            model.Add(pos_diff >= 0)  # Ensure pos_diff is non-negative
            
            model.Add(neg_diff >= pessObj.get((d, s), 0) - assigned_workers)  # If shortfall, neg_diff > 0
            model.Add(neg_diff >= 0)  # Ensure neg_diff is non-negative
            
            # Add both positive and negative deviations to the objective function
            objective_terms.append(1000 * pos_diff)
            objective_terms.append(1000 * neg_diff)
```

**Lógica de penalização**:
- **Desvios lineares**: penalização proporcional ao desvio


### 2.2 Penalizações por Violação de Staffing Crítico

Penaliza soluções onde o número de trabalhadores é 0 em 300 pontos.
Penaliza soluções onde o número de trabalhadores é inferior ao mínimo em 60 pontos.

```python

HEAVY_PENALTY = 300  # Penalty for days with no workers
MIN_WORKER_PENALTY = 60  # Penalty for breaking minimum worker requirements

# Penalização por dias completamente sem trabalhadores
for d in days_of_year:
    if d not in closed_holidays:  # Skip closed holidays
        for s in working_shift:
            if pessObj.get((d, s), 0) > 0:  # Only penalize when pessObj exists
                # Calculate the number of assigned workers for this day and shift
                assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
                
                # Create a boolean variable to indicate if there are no workers
                no_workers = model.NewBoolVar(f"no_workers_{d}_{s}")
                model.Add(assigned_workers == 0).OnlyEnforceIf(no_workers)
                model.Add(assigned_workers >= 1).OnlyEnforceIf(no_workers.Not())
                
                # Store the variable
                no_workers_penalties[(d, s)] = no_workers
                
                # Add a heavy penalty to the objective function
                objective_terms.append(HEAVY_PENALTY * no_workers)

# Penalização por staffing abaixo do mínimo
for d in days_of_year:
    for s in working_shift:
        min_req = min_workers.get((d, s), 0)
        if min_req > 0:  # Only penalize when there's a minimum requirement
            # Calculate the number of assigned workers for this day and shift
            assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
            
            # Create a variable to represent the shortfall from the minimum
            shortfall = model.NewIntVar(0, min_req, f"min_shortfall_{d}_{s}")
            model.Add(shortfall >= min_req - assigned_workers)
            model.Add(shortfall >= 0)
            
            # Store the variable
            min_workers_penalties[(d, s)] = shortfall
            
            # Add penalty to the objective function
            objective_terms.append(MIN_WORKER_PENALTY * shortfall)

```

**Escalas de penalização**:
- **Dias sem trabalhadores**: 300 × número de dias 
- **Staffing insuficiente**: 60 × défice de pessoas 

### 2.3 Penalização por Colaboradores do tipo Manager e Keyholder com Folgas no mesmo Dia 

Evita que gestores (managers) e keyholder (keyholders) estejam simultaneamente de folga no mesmo dia, garantindo sempre a presença de responsabilidades críticas operacionais.

**O que faz**: Penaliza situações onde pelo menos um gestor E pelo menos um keyholder estão ambos de folga (L ou LQ) no mesmo dia

**Custo se violado**: Ausência simultânea de supervisão hierárquica e controlo de acesso físico, criando risco operacional crítico e de segurança

**Variáveis**: 
- `mgr_any[(d)]` - boolean indicando se algum gestor está de folga no dia d
- `kh_any[(d)]` - boolean indicando se algum keyholder está de folga no dia d  
- `both_off[(d)]` - boolean para sobreposição crítica de responsabilidades

**Implementação**:
```python
PEN_MGR_KH_SAME_OFF = 30000

# Para cada dia (excluindo feriados encerrados)
for d in days_of_year:
    if d in closed:
        continue

    # Calcular somas de folgas por grupo
    mgr_sum = sum(sum(shift.get((w, d, lab), 0) for lab in ["L", "LQ"]) 
                  for w in managers if d in days_of_year)
    
    kh_sum = sum(sum(shift.get((w, d, lab), 0) for lab in ["L", "LQ"]) 
                 for w in keyholders if d in days_of_year)

    # Variáveis booleanas para detecção de folgas por grupo
    mgr_any = model.NewBoolVar(f"mgr_any_{d}")
    kh_any = model.NewBoolVar(f"kh_any_{d}")

    # Ligar somas às variáveis booleanas
    model.Add(mgr_sum >= 1).OnlyEnforceIf(mgr_any)
    model.Add(mgr_sum == 0).OnlyEnforceIf(mgr_any.Not())
    
    model.Add(kh_sum >= 1).OnlyEnforceIf(kh_any)
    model.Add(kh_sum == 0).OnlyEnforceIf(kh_any.Not())

    # Variável para sobreposição crítica
    both_off = model.NewBoolVar(f"mgr_kh_both_off_{d}")
    
    # both_off = 1 sse mgr_any = 1 E kh_any = 1
    model.AddBoolAnd([mgr_any, kh_any]).OnlyEnforceIf(both_off)
    model.AddBoolOr([mgr_any.Not(), kh_any.Not()]).OnlyEnforceIf(both_off.Not())

    # Adicionar penalização crítica
    objective_terms.append(PEN_MGR_KH_SAME_OFF * both_off)
```

**Peso**: 30000 - penalização extremamente alta, refletindo a criticidade de manter sempre pelo menos uma das responsabilidades operacionais presentes

**Lógica operacional**:
- **Gestores**: supervisão, decisões críticas, resolução de problemas
- **Redundância crítica**: pelo menos um dos dois grupos deve estar sempre presente

**Interação com outras penalizações**:
- **Maior prioridade**: peso superior às outras penalizações 
- **Flexibilidade individual**: não impede folgas, apenas a sobreposição total
- **Aplicação seletiva**: apenas em dias úteis, excluindo feriados encerrados

### 2.4 Controle de Sobreposição de keyholder

Evita que múltiplos keyholder estejam de folga simultaneamente no mesmo dia, garantindo sempre disponibilidade de acesso às instalações.

**O que faz**: Penaliza situações onde 2 ou mais keyholder estão de folga (L ou LQ) no mesmo dia

**Custo se violado**: Restrição severa de acesso às instalações, possível paralização operacional por falta de controlo de acessos com custo de 50000

**Variáveis**: 
- `kh_overlap[(d)]` - boolean indicando se 2+ keyholder estão de folga no dia d

**Implementação**:
```python
PEN_KH_OVERLAP = 50000

for d in days_of_year:
    if d in closed:
        continue

    # Calcular soma de folgas de keyholder
    kh_sum = sum(sum(shift.get((w, d, lab), 0) for lab in ["L", "LQ"]) 
                 for w in keyholders if d in days_of_year)

    # Variável para sobreposição de keyholder
    kh_overlap = model.NewBoolVar(f"kh_overlap_{d}")
    
    # kh_overlap = 1 sse kh_sum >= 2 (múltiplos keyholder de folga)
    model.Add(kh_sum >= 2).OnlyEnforceIf(kh_overlap)
    model.Add(kh_sum <= 1).OnlyEnforceIf(kh_overlap.Not())
    
    objective_terms.append(PEN_KH_OVERLAP * kh_overlap)
```

**Peso**: 50000 - penalização crítica por risco operacional de acesso

### 2.5 Controle de Sobreposição de Gestores

Evita que múltiplos gestores estejam de folga simultaneamente no mesmo dia, garantindo sempre disponibilidade de supervisão.

**O que faz**: Penaliza situações onde 2 ou mais gestores estão de folga (L ou LQ) no mesmo dia

**Custo se violado**: Falta de supervisão adequada, ausência de tomada de decisões críticas e liderança operacional

**Variáveis**: 
- `mgr_overlap[(d)]` - boolean indicando se 2+ gestores estão de folga no dia d

**Implementação**:
```python
PEN_MGR_OVERLAP = 50000

for d in days_of_year:
    if d in closed:
        continue

    # Calcular soma de folgas de gestores
    mgr_sum = sum(sum(shift.get((w, d, lab), 0) for lab in ["L", "LQ"]) 
                  for w in managers if d in days_of_year)

    # Variável para sobreposição de gestores
    mgr_overlap = model.NewBoolVar(f"mgr_overlap_{d}")
    
    # mgr_overlap = 1 sse mgr_sum >= 2 (múltiplos gestores de folga)
    model.Add(mgr_sum >= 2).OnlyEnforceIf(mgr_overlap)
    model.Add(mgr_sum <= 1).OnlyEnforceIf(mgr_overlap.Not())
    
    objective_terms.append(PEN_MGR_OVERLAP * mgr_overlap)
```

**Peso**: 50000 - penalização crítica por risco de gestão

**Hierarquia completa de responsabilidades críticas**:
1. **Sobreposição Manager + Keyholder** (30000): impede ausência total de responsabilidades
2. **Sobreposição múltiplos Keyholders** (50000): garante acesso às instalações
3. **Sobreposição múltiplos Managers** (50000): garante supervisão operacional

## 3. Objetivos de Bem-estar

### 3.1 Incentivo a Dias de Folga Consecutivos

Incentiva atribuição de dias de folga consecutivos com um peso de -1.

```python
consecutive_free_day_bonus = []
for w in workers:
    all_work_days = sorted(working_days[w])
    
    # Create boolean variables for each day indicating if it's a free day
    free_day_vars = {}
    for d in all_work_days:
        free_day = model.NewBoolVar(f"free_day_{w}_{d}")
        
        # Sum the L, F, LQ, A, V shifts for this day
        free_shift_sum = sum(
            shift.get((w, d, shift_type), 0) 
            for shift_type in ["L", "F", "LQ", "A", "V"]
        )
        
        # Link the boolean variable to whether any free shift is assigned
        model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
        model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())
        
        free_day_vars[d] = free_day
    
    # For each pair of consecutive days in the worker's schedule
    for i in range(len(all_work_days) - 1):
        day1 = all_work_days[i]
        day2 = all_work_days[i+1]
        
        # Only consider consecutive calendar days
        if day2 == day1 + 1:
            # Create a boolean variable for consecutive free days
            consecutive_free = model.NewBoolVar(f"consecutive_free_{w}_{day1}_{day2}")
            
            # Both days must be free for the bonus to apply
            model.AddBoolAnd([free_day_vars[day1], free_day_vars[day2]]).OnlyEnforceIf(consecutive_free)
            model.AddBoolOr([free_day_vars[day1].Not(), free_day_vars[day2].Not()]).OnlyEnforceIf(consecutive_free.Not())
            
            # Add a negative term (bonus) to the objective function for each consecutive free day pair
            consecutive_free_day_bonus.append(consecutive_free)
# Add the bonus term to the objective with appropriate weight (negative to minimize)
# Using a weight of -1 to prioritize consecutive free days
objective_terms.extend([-1 * term for term in consecutive_free_day_bonus])
```

**Benefícios**:
- **Qualidade de vida**: períodos de descanso contínuos

### 3.2 Balanceamento de Folgas ao Domingo ao longo do ano

Tenta separar as folgas ao domingo ao longo do ano. Para tal, cria 5 janelas temporais durante o ano e tenta equilibrar o número de folgas das diferentes janelas. Por cada domingo numa janela temporal que seja superior ao ideal, a solução é penalizada em 1 ponto. O valor ideal de folgas em cada janela temporal é calculado utilizando:

                base_ideal = total_sunday_free // num_segments
                remainder = total_sunday_free % num_segments
                # First 'remainder' segments get one extra
                ideal_count = base_ideal + (1 if segment < remainder else 0)

```python
SUNDAY_BALANCE_PENALTY = 1  # Weight for Sunday balance penalty
sunday_balance_penalties = []


for w in workers:
    worker_sundays = [d for d in sundays if d in working_days[w]]
    
    if len(worker_sundays) <= 1:
        continue  # Skip if worker has 0 or 1 Sunday (no balancing needed)
    
    # Create variables for Sunday free days (L shifts)
    sunday_free_vars = []
    for sunday in worker_sundays:
        sunday_free = model.NewBoolVar(f"sunday_free_{w}_{sunday}")
        
        # Link to actual L shift assignment
        model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) >= 1).OnlyEnforceIf(sunday_free)
        model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 0).OnlyEnforceIf(sunday_free.Not())
        sunday_free_vars.append(sunday_free)
    
    # Calculate target spacing between Sunday free days
    total_sunday_free = len(sunday_free_vars)
    
    # For even distribution, we want to minimize variance in spacing
    # We'll divide the year into segments and try to have roughly equal distribution
    num_segments = min(5, total_sunday_free)  # Use 5 segments or fewer if not enough Sundays
    if num_segments > 1:
        segment_size = total_sunday_free // num_segments
        for segment in range(num_segments):
            start_idx = segment * segment_size
            end_idx = (segment + 1) * segment_size if segment < num_segments - 1 else total_sunday_free
            
            segment_sundays = sunday_free_vars[start_idx:end_idx]
            
            if len(segment_sundays) > 0:
                # Create variables for deviation from ideal distribution
                segment_free_count = sum(segment_sundays)
                
                # Handle remainder when total doesn't divide evenly
                base_ideal = total_sunday_free // num_segments
                remainder = total_sunday_free % num_segments
                # First 'remainder' segments get one extra
                ideal_count = base_ideal + (1 if segment < remainder else 0)
                
                # Maximum possible deviation bounds
                max_over = len(segment_sundays)  # All Sundays in segment could be free
                max_under = ideal_count  # Could have 0 instead of ideal_count
                
                # Create penalty variables for over/under allocation
                over_penalty = model.NewIntVar(0, max_over, f"sunday_over_{w}_{segment}")
                under_penalty = model.NewIntVar(0, max_under, f"sunday_under_{w}_{segment}")
                
                # Correctly calculate deviations (handling negative cases)
                model.Add(over_penalty >= segment_free_count - ideal_count)
                model.Add(over_penalty >= 0)  # Ensure non-negative
                
                model.Add(under_penalty >= ideal_count - segment_free_count)
                model.Add(under_penalty >= 0)  # Ensure non-negative
                
                sunday_balance_penalties.append(SUNDAY_BALANCE_PENALTY * over_penalty)
                sunday_balance_penalties.append(SUNDAY_BALANCE_PENALTY * under_penalty)

objective_terms.extend(sunday_balance_penalties)

```

### 3.3 Balanceamento de Fins de Semana de Folga ao longo do ano

Tenta separar as folgas de fim de semana ao longo do ano. Para tal, cria 5 janelas temporais durante o ano e tenta equilibrar o número de folgas das diferentes janelas. Por cada fim de semana de folga numa janela temporal que seja superior ao ideal, a solução é penalizada em 8 ponto. O valor ideal de folgas em cada janela temporal é calculado utilizando:

                base_ideal = max_possible_quality // num_segments
                remainder = max_possible_quality % num_segments
                ideal_count = base_ideal + (1 if segment < remainder else 0)

```python
C2D_BALANCE_PENALTY = 8  # Weight for c2d balance penalty
c2d_balance_penalties = []
quality_weekend_2_dict = {}
for w in workers:
    # Find all potential quality weekends (Saturday-Sunday pairs)
    quality_weekend_vars = []
    weekend_dates = []
    
    for sunday in sundays:
        saturday = sunday - 1
        
        # Check if both Saturday and Sunday are in worker's schedule
        if saturday in working_days[w] and sunday in working_days[w]:
            # Create boolean for this quality weekend
            quality_weekend = model.NewBoolVar(f"quality_weekend_{w}_{sunday}")
            
            # Quality weekend is True if LQ on Saturday AND L on Sunday
            has_lq_saturday = model.NewBoolVar(f"has_lq_sat_{w}_{saturday}")
            has_l_sunday = model.NewBoolVar(f"has_l_sun_{w}_{sunday}")
            
            # Link to actual shift assignments
            model.Add(shift.get((w, saturday, "LQ"), 0) >= 1).OnlyEnforceIf(has_lq_saturday)
            model.Add(shift.get((w, saturday, "LQ"), 0) == 0).OnlyEnforceIf(has_lq_saturday.Not())
            
            model.Add(shift.get((w, sunday, "L"), 0) >= 1).OnlyEnforceIf(has_l_sunday)
            model.Add(shift.get((w, sunday, "L"), 0) == 0).OnlyEnforceIf(has_l_sunday.Not())
            
            # Quality weekend requires both conditions
            model.AddBoolAnd([has_lq_saturday, has_l_sunday]).OnlyEnforceIf(quality_weekend)
            model.AddBoolOr([has_lq_saturday.Not(), has_l_sunday.Not()]).OnlyEnforceIf(quality_weekend.Not())
            quality_weekend_2_dict[(w, sunday)] = quality_weekend
            
            quality_weekend_vars.append(quality_weekend)
            weekend_dates.append(sunday)
    
    if len(quality_weekend_vars) <= 1:
        continue  # Skip if worker has 0 or 1 potential quality weekend
    
    # Divide the year into segments and try to distribute quality weekends evenly
    num_segments = min(5, len(quality_weekend_vars))  # Use 5 segments or fewer if not enough weekends
    if num_segments > 1:
        segment_size = len(quality_weekend_vars) // num_segments
        
        for segment in range(num_segments):
            start_idx = segment * segment_size
            end_idx = (segment + 1) * segment_size if segment < num_segments - 1 else len(quality_weekend_vars)
            
            segment_weekends = quality_weekend_vars[start_idx:end_idx]
            
            if len(segment_weekends) > 0:
                segment_count = sum(segment_weekends)
                max_possible_quality = c2d.get(w,0)  # from your business logic
                base_ideal = max_possible_quality // num_segments
                remainder = max_possible_quality % num_segments
                ideal_count = base_ideal + (1 if segment < remainder else 0)
                
                # Maximum possible deviation bounds
                max_over = len(segment_weekends)  # All weekends in segment could be quality
                max_under = ideal_count  # Could have 0 instead of ideal_count
                
                # Create penalty variables for deviation from ideal distribution
                over_penalty = model.NewIntVar(0, max_over, f"c2d_over_{w}_{segment}")
                under_penalty = model.NewIntVar(0, max_under, f"c2d_under_{w}_{segment}")
                
                # Correctly calculate deviations (handling negative cases)
                model.Add(over_penalty >= segment_count - ideal_count)
                model.Add(over_penalty >= 0)  # Ensure non-negative
                
                model.Add(under_penalty >= ideal_count - segment_count)
                model.Add(under_penalty >= 0)  # Ensure non-negative
                
                c2d_balance_penalties.append(C2D_BALANCE_PENALTY * over_penalty)
                c2d_balance_penalties.append(C2D_BALANCE_PENALTY * under_penalty)

objective_terms.extend(c2d_balance_penalties)
```


## 4. Objetivos de Consistência

### 4.1 Penalização de Inconsistência de Turnos Semanais

Procura atribuir o mesmo tipo de turno (M e T) durante uma semana para um certo trabalhador. Penaliza em 3 pontos a solução caso um colaborador tenha turnos diferentes numa semana.

```python
INCONSISTENT_SHIFT_PENALTY = 3

for w in workers:
    for week in week_to_days.keys():  # Iterate over all weeks
        days_in_week = week_to_days[week]
        working_days_in_week = [d for d in days_in_week if d in working_days.get(w, [])]
        
        if len(working_days_in_week) >= 2:  # Only if worker has at least 2 working days this week
            # Create variables to track if the worker has M or T shifts this week
            has_m_shift = model.NewBoolVar(f"has_m_shift_{w}_{week}")
            has_t_shift = model.NewBoolVar(f"has_t_shift_{w}_{week}")
            
            # Create expressions for total M and T shifts this week
            total_m = sum(shift.get((w, d, "M"), 0) for d in working_days_in_week)
            total_t = sum(shift.get((w, d, "T"), 0) for d in working_days_in_week)
            
            # Worker has M shifts if total_m > 0
            model.Add(total_m >= 1).OnlyEnforceIf(has_m_shift)
            model.Add(total_m == 0).OnlyEnforceIf(has_m_shift.Not())
            
            # Worker has T shifts if total_t > 0
            model.Add(total_t >= 1).OnlyEnforceIf(has_t_shift)
            model.Add(total_t == 0).OnlyEnforceIf(has_t_shift.Not())
            
            # Create a variable to indicate inconsistent shifts
            inconsistent_shifts = model.NewBoolVar(f"inconsistent_shifts_{w}_{week}")
            
            # Worker has inconsistent shifts if both M and T shifts exist
            model.AddBoolAnd([has_m_shift, has_t_shift]).OnlyEnforceIf(inconsistent_shifts)
            model.AddBoolOr([has_m_shift.Not(), has_t_shift.Not()]).OnlyEnforceIf(inconsistent_shifts.Not())
        
            # Store the variable
            inconsistent_shift_penalties[(w, week)] = inconsistent_shifts
            
            # Add penalty to the objective function
            objective_terms.append(INCONSISTENT_SHIFT_PENALTY * inconsistent_shifts)
```

**Benefício operacional**:
- **Simplicidade de gestão**: colaboradores  consistentes por semana
- **Flexibilidade entre semanas**: pode mudar semana a semana


### 4.2 Balanceamento de Folgas ao Domingo Entre Colaboradores

Procura equilibrar número de domingos entre colaboradores. Por cada folga a mais que um colaborador tenha relativamente a outro, a solução é penalizada em 50 pontos.

```python
SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY = 50
sunday_balance_across_workers_penalties = []
# Create constraint variables for each worker's total Sunday free days
sunday_free_worker_vars = {}
workers_with_sundays = [] 
for w in workers:
    worker_sundays = [d for d in sundays if d in working_days[w]]
    
    if len(worker_sundays) == 0:
        continue  # Skip workers with no Sundays
    
    workers_with_sundays.append(w)
    
    # Create variables for Sunday free days
    sunday_free_vars = []
    for sunday in worker_sundays:
        sunday_free = model.NewBoolVar(f"sunday_free_{w}_{sunday}")
        
        # Link to actual L or F shift assignment
        model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) >= 1).OnlyEnforceIf(sunday_free)
        model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 0).OnlyEnforceIf(sunday_free.Not())
        
        sunday_free_vars.append(sunday_free)
    
    # Create constraint variable for total Sunday free days
    total_sunday_free_var = model.NewIntVar(0, len(worker_sundays), f"total_sunday_free_{w}")
    model.Add(total_sunday_free_var == sum(sunday_free_vars))
    
    sunday_free_worker_vars[w] = total_sunday_free_var
# STRATEGY: Pairwise proportional balance (simplest and most reliable)
if len(workers_with_sundays) > 1:
    # For each pair of workers, ensure proportional fairness
    for i, w1 in enumerate(workers_with_sundays):
        for w2 in workers_with_sundays[i+1:]:
            if last_day.get(w1, 0) == 0 :
                last_day[w1] = days_of_year[-1]
            if last_day.get(w2, 0) == 0 :
                last_day[w2] = days_of_year[-1]
            prop1 = (last_day.get(w1, 0) - first_day.get(w1, 0) + 1) / len(days_of_year)
            prop1 = max(0.0, min(1.0, prop1))
            prop2 = (last_day.get(w2, 0) - first_day.get(w2, 0) + 1) / len(days_of_year)
            prop2 = max(0.0, min(1.0, prop2))
            #logger.info(f"Worker {w1} proportion: {prop1}, first day: {first_day.get(w1, 0)}, last day: {last_day.get(w1, 0)}, Worker {w2} proportion: {prop2}, first day: {first_day.get(w2, 0)}, last day: {last_day.get(w2, 0)}")
            if prop1 > 0 and prop2 > 0:
                # Calculate proportion ratio as integers (multiply by 100 for precision)
                prop1_int = int(prop1 * 100)
                prop2_int = int(prop2 * 100)
                
                # Calculate maximum possible difference
                max_sundays_w1 = len([d for d in sundays if d in working_days[w1]])
                max_sundays_w2 = len([d for d in sundays if d in working_days[w2]])
                max_diff = max(max_sundays_w1 * prop2_int, max_sundays_w2 * prop1_int)
                
                # Create variables for proportional difference
                proportional_diff_pos = model.NewIntVar(0, max_diff, f"prop_diff_pos_{w1}_{w2}")
                proportional_diff_neg = model.NewIntVar(0, max_diff, f"prop_diff_neg_{w1}_{w2}")
                
                # Proportional balance constraint:
                # sunday_free_worker_vars[w1] / prop1 should ≈ sunday_free_worker_vars[w2] / prop2
                # Rearranged: sunday_free_worker_vars[w1] * prop2_int should ≈ sunday_free_worker_vars[w2] * prop1_int
                
                model.Add(proportional_diff_pos >= 
                        sunday_free_worker_vars[w1] * prop2_int - sunday_free_worker_vars[w2] * prop1_int)
                model.Add(proportional_diff_pos >= 0)
                
                model.Add(proportional_diff_neg >= 
                        sunday_free_worker_vars[w2] * prop1_int - sunday_free_worker_vars[w1] * prop2_int)
                model.Add(proportional_diff_neg >= 0)
                
                # Add penalties for proportional imbalance
                weight = SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY // 2  # Distribute penalty across pairs
                sunday_balance_across_workers_penalties.append(weight * proportional_diff_pos)
                sunday_balance_across_workers_penalties.append(weight * proportional_diff_neg)
# Add to objective
objective_terms.extend(sunday_balance_across_workers_penalties)  
```

### 4.3 Balanceamento de LQ Entre Colaboradores

Procura equilibrar número de fins de semana de folga entre colaboradores. Por cada fim de semana de folga a mais que um colaborador tenha relativamente a outro, a solução é penalizada em 50 pontos.

```python
LQ_BALANCE_ACROSS_WORKERS_PENALTY = 50
lq_balance_across_workers_penalties = []
lq_free_worker_vars = {}
workers_with_lq = []
saturdays = [s - 1 for s in sundays if (s - 1) in days_of_year]
for w in workers:
    # Only consider weekends where the worker is actually exposed:
    # both Saturday and the following Sunday exist in their working_days.
    eligible_saturdays = [s for s in saturdays if (s in working_days[w] and (s + 1) in working_days[w])]
    if not eligible_saturdays:
        continue
    workers_with_lq.append(w)
    lq_free_vars = []
    for s in eligible_saturdays:
        d = s + 1  # following Sunday
        # --- Saturday LQ flag ---
        # Use the existing shift var if available; otherwise create a dummy Bool forced to 0.
        if (w, s, "LQ") in shift:
            lq_sat = shift[(w, s, "LQ")]
        else:
            lq_sat = model.NewBoolVar(f"lq_sat_{w}_{s}")
            model.Add(lq_sat == 0)  
        # --- Sunday must be  "L" ---
        if (w, d, "L") in shift:
            sun_is_L = shift[(w, d, "L")]
        else:
            sun_is_L = model.NewBoolVar(f"sun_is_L_{w}_{d}")
            model.Add(sun_is_L == 0)  
        
        if (w, d, "F") in shift:
            sun_is_F = shift[(w, d, "F")]
        else:
            sun_is_F = model.NewBoolVar(f"sun_is_F_{w}_{d}")
            model.Add(sun_is_F == 0)
        # --- Weekend LQ indicator (AND of Saturday LQ and Sunday L) ---
        
        lq_weekend = model.NewBoolVar(f"lq_weekend_{w}_{s}_{d}")
        model.AddMultiplicationEquality(lq_weekend, [lq_sat, sun_is_L])
        lq_free_vars.append(lq_weekend)
    # Total LQ weekends per worker (bounded by the number of eligible weekends)
    total_lq_free_var = model.NewIntVar(0, len(lq_free_vars), f"total_lq_free_{w}")
    model.Add(total_lq_free_var == sum(lq_free_vars))
    lq_free_worker_vars[w] = total_lq_free_var
if len(workers_with_lq) > 1:
    for i, w1 in enumerate(workers_with_lq):
        for w2 in workers_with_lq[i+1:]:
            # Keep your existing 'proportion' for consistency.
            # If you compute a specific LQ exposure (prop_lq), you can swap it here.
            if last_day.get(w1, 0) == 0 :
                last_day[w1] = days_of_year[-1]
            if last_day.get(w2, 0) == 0 :
                last_day[w2] = days_of_year[-1]
            prop1 = (last_day.get(w1, 0) - first_day.get(w1, 0) + 1) / len(days_of_year)
            prop1 = max(0.0, min(1.0, prop1))
            prop2 = (last_day.get(w2, 0) - first_day.get(w2, 0) + 1) / len(days_of_year)
            prop2 = max(0.0, min(1.0, prop2))
            if prop1 <= 0 or prop2 <= 0:
                continue
            # Integer scaling avoids division and floating-point issues
            prop1_int = int(prop1 * 100)
            prop2_int = int(prop2 * 100)
            max_w1 = len([s for s in saturdays if (s in working_days[w1] and (s + 1) in working_days[w1])])
            max_w2 = len([s for s in saturdays if (s in working_days[w2] and (s + 1) in working_days[w2])])
            max_diff = max(max_w1 * prop2_int, max_w2 * prop1_int)
            diff_pos = model.NewIntVar(0, max_diff, f"lq_prop_diff_pos_{w1}_{w2}")
            diff_neg = model.NewIntVar(0, max_diff, f"lq_prop_diff_neg_{w1}_{w2}")
            # Normalize without division by comparing: c1*prop2 ≈ c2*prop1
            # Compare normalized counts without divisions: c1*prop2 ≈ c2*prop1
            model.Add(diff_pos >= lq_free_worker_vars[w1] * prop2_int - lq_free_worker_vars[w2] * prop1_int)
            model.Add(diff_neg >= lq_free_worker_vars[w2] * prop1_int - lq_free_worker_vars[w1] * prop2_int)
            weight = LQ_BALANCE_ACROSS_WORKERS_PENALTY // 2
            lq_balance_across_workers_penalties.append(weight * diff_pos)
            lq_balance_across_workers_penalties.append(weight * diff_neg)
# Add to objective
objective_terms.extend(lq_balance_across_workers_penalties) 
```



## 5. Construção da Função Objectivo Final

### 5.1 Agregação e Optimização
```python
# Criar função objectivo final
total_objective = sum(objective_terms)
model.Minimize(total_objective)

# Retornar variáveis auxiliares para debugging
return debug_vars
```

Retorna qualquer varíavel do modelo necessária para possível debugging. Basta associar a variável aa debug_vars.
Ex:

```python
debug_vars[f"kh_overlap_{d}"] = kh_overlap
```

E depois passar debug_vars na função solve de salsaAlgorithm

### 5.2 Hierarquia de Prioridades (por peso)
1. **Folgas Exclusivas entre colaboradores do mesmo nível (Managers e Keyholders)**: 50000
2. **Folgas Exclusivas entre colaboradores de níveis diferentes (Managers e Keyholders)**: 30000
3. **Necessidades diárias**: 1000
4. **Dias sem trabalhadores**: 300 
5. **Staffing insuficiente**: 60 
6. **Balanceamento Domingo e Fins de Semana de Folga entre colaboradores**: 50 
7. **Balanceamento C2D ao longo do Ano**: 8
8. **Inconsistência semanal**: 3 
9. **Balanceamento Domingos de Folga ao longo do Ano**: 1
10. **Folgas consecutivas**: -1 

### 5.3 Análise de Conflitos e Trade-offs
- **Operacional vs. Bem-estar**: staffing mínimo vs. folgas consecutivas
- **Individual vs. Coletivo**: necessidades específicas vs. equidade entre colaboradores
- **Curto vs. Longo prazo**: necessidades diárias vs. padrões semanais
- **Flexibilidade vs. Consistência**: adaptação vs. previsibilidade

O sistema de pesos permite ajustar estes trade-offs conforme as prioridades organizacionais, criando soluções que equilibram eficácia operacional com satisfação dos colaboradores.

---

# Solver

O processo de resolução é executado pela função `solve()` que utiliza o OR-Tools CP-SAT solver, um dos mais avançados solvers de constraint programming disponíveis. Esta seção detalha todos os aspectos da configuração, execução e processamento de resultados.

## 1. Arquitetura da Função `solve()`

### 1.1 Assinatura e Parâmetros de Entrada

```python
def solve(
    model: cp_model.CpModel, 
    days_of_year: List[int], 
    workers: List[int], 
    special_days: List[int], 
    shift: Dict[Tuple[int, int, str], cp_model.IntVar], 
    shifts: List[str],
    max_time_seconds: int = 600,
    enumerate_all_solutions: bool = False,
    use_phase_saving: bool = True,
    log_search_progress: bool = 0,
    log_callback: Optional[Callable[[str], None]] = None,
    output_filename: str = os.path.join(ROOT_DIR, 'data', 'output', 'working_schedule.xlsx'),
    debug_vars: Optional[Dict[str, cp_model.IntVar]] = None
) -> pd.DataFrame:
```

**Parâmetros de entrada detalhados**:
- **`model`**: Instância CP-SAT já configurada com variáveis, restrições e função objetivo
- **`days_of_year`**: Lista de dias a escalonar (tipicamente 1-365)
- **`workers`**: Lista de IDs dos trabalhadores (ex: [80001676, 80001677, ...])
- **`special_days`**: Lista de dias especiais (domingos, feriados)
- **`shift`**: Dicionário de variáveis de decisão {(worker, day, shift_type): BoolVar}
- **`shifts`**: Lista de tipos de turno disponíveis ["M", "T", "L", "LQ", "F", "A", "V", "-"]
- **`max_time_seconds`**: Limite máximo de tempo de execução (padrão: 600s)
- **`enumerate_all_solutions`**: Se deve enumerar todas as soluções (padrão: False)
- **`use_phase_saving`**: Otimização de busca (padrão: True)
- **`log_search_progress`**: Nível de logging (0=nenhum, 1=básico, 2=detalhado)
- **`debug_vars`**: Variáveis auxiliares para debugging da otimização

## 2. Validação de Entrada

### 2.1 Validação de Tipos e Consistência
```python
# Validação rigorosa de todos os parâmetros de entrada
if not isinstance(model, cp_model.CpModel):
    error_msg = f"model must be a CP-SAT CpModel instance. model: {model}, type: {type(model)}"
    logger.error(error_msg)
    raise ValueError(error_msg)

if not days_of_year or not isinstance(days_of_year, list):
    error_msg = f"days_of_year must be a non-empty list. days_of_year: {days_of_year}"
    logger.error(error_msg)
    raise ValueError(error_msg)
```

**Validações implementadas**:
- Verificação de tipos de todos os parâmetros
- Consistência entre listas (workers, days_of_year)
- Validação de estrutura do dicionário shift
- Verificação de limites temporais válidos

### 2.2 Logging de Estatísticas Iniciais
```python
logger.info(f"  - Days to schedule: {len(days_of_year)} days (from {min(days_of_year)} to {max(days_of_year)})")
logger.info(f"  - Workers: {len(workers)} workers")
logger.info(f"  - Special days: {len(special_days)} days")
logger.info(f"  - Available shifts: {shifts}")
logger.info(f"  - Decision variables: {len(shift)} variables")
logger.info(f"  - Max solving time: {max_time_seconds} seconds")
```

## 3. Configuração do Solver CP-SAT

### 3.1 Instanciação e Configuração Básica
```python
solver = cp_model.CpSolver()
start_time = datetime.now()
```

### 3.2 Parâmetros de Performance

#### **Paralelização**
```python
solver.parameters.num_search_workers = 8
```
- **Função**: Define número de threads paralelos para busca
- **Valores possíveis**: 1-32 (limitado pelo hardware)
- **Padrão SALSA**: 8 workers
- **Impacto**: Maior paralelização pode acelerar busca em problemas complexos
- **Trade-off**: Mais workers = mais memória consumida

#### **Gestão de Tempo**
```python
solver.parameters.max_time_in_seconds = 600
```
- **Função**: Limite máximo de execução antes de timeout
- **Valores possíveis**: 1-∞ segundos
- **Padrão SALSA**: 600 segundos (10 minutos)
- **Recomendações**: 60-300s para testes, 600-3600s para produção
- **Comportamento**: Solver retorna melhor solução encontrada até ao timeout

#### **Logging e Monitorização**
```python
solver.parameters.log_search_progress = log_search_progress
```
- **Função**: Controla verbosidade do logging durante busca
- **Valores possíveis**: 
  - `0`: Sem logging
  - `1`: Logging básico (progress updates)
  - `2`: Logging detalhado (decisões, backtracks)
- **Padrão SALSA**: Configurável via parâmetro
- **Uso**: 0 para produção, 1-2 para debugging

#### **Phase Saving**
```python
solver.parameters.use_phase_saving = use_phase_saving
```
- **Função**: Otimização que reutiliza decisões de buscas anteriores
- **Valores possíveis**: `True`/`False`
- **Padrão SALSA**: `True`
- **Benefício**: Acelera busca em problemas com estrutura repetitiva
- **Custo**: Ligeiro aumento de memória

### 3.3 Otimizações Avançadas

#### **Presolve**
```python
solver.parameters.cp_model_presolve = True
```
- **Função**: Simplificação automática do modelo antes da busca
- **Operações**: Remove variáveis redundantes, simplifica constraints, detecta infeasibilidades
- **Impacto**: Reduz significativamente o espaço de busca
- **Sempre recomendado**: True para todos os casos

#### **Probing Level**
```python
solver.parameters.cp_model_probing_level = 3
```
- **Função**: Nível de análise de implicações lógicas. Tenta perceber se diferentes restrições estão ligadas entre si.
- **Valores possíveis**: 0-4
  - `0`: Sem probing
  - `1`: Probing básico
  - `2`: Probing moderado
  - `3`: Probing avançado (padrão SALSA)
  - `4`: Probing exaustivo
- **Trade-off**: Maior nível = melhor simplificação, mas mais tempo de presolve

#### **Symmetry Breaking**
```python
solver.parameters.symmetry_level = 4
```
- **Função**: Detecção e quebra de simetrias no modelo.
- **Valores possíveis**: 0-4
  - `0`: Sem detecção de simetrias
  - `1-3`: Níveis crescentes de detecção
  - `4`: Detecção máxima (padrão SALSA)
- **Benefício**: Elimina ramos simétricos da busca
- **Especialmente útil**: Em problemas de scheduling com trabalhadores equivalentes

#### **Linearização**
```python
solver.parameters.linearization_level = 2
```
- **Função**: Converte constraints não-lineares em relaxações lineares
- **Valores possíveis**: 0-2
  - `0`: Sem linearização
  - `1`: Linearização básica
  - `2`: Linearização avançada (padrão SALSA)
- **Benefício**: Melhora bounds e acelera busca
- **Aplicação**: Útil em funções objetivo complexas como no SALSA

### 3.4 Configuração de Teste vs Produção
```python
testing = False
if testing == True:
    solver.parameters.random_seed = 42
```
- **Função**: Torna resultados reproduzíveis para testes
- **Uso**: Apenas durante desenvolvimento/debug
- **Produção**: Sem seed para explorar diferentes soluções

## 4. Callback de Solução

### 4.1 Inicialização do Callback
```python
solution_callback = SolutionCallback(logger, shift, workers, days_of_year)
```

### 4.2 Funcionalidades do `SolutionCallback`

#### **Monitorização em Tempo Real**
```python
class SolutionCallback(cp_model.CpSolverSolutionCallback):
    def __init__(self, logger, shift_vars, workers, days_of_year):
        self.solution_count = 0
        self.start_time = time.time()
        self.best_objective = float('inf')
```

#### **Callback de Cada Solução**
```python
def on_solution_callback(self):
    current_time = time.time()
    elapsed_time = current_time - self.start_time
    self.solution_count += 1
    current_objective = self.ObjectiveValue()
    best_bound = self.BestObjectiveBound()
    
    # Cálculo do gap de otimalidade
    if current_objective != 0:
        gap_percent = ((current_objective - best_bound) / abs(current_objective)) * 100
    else:
        gap_percent = 0.0
```

**Métricas reportadas**:
- **Número da solução**: Contador sequencial
- **Tempo decorrido**: Desde início da busca
- **Valor objetivo atual**: Função objetivo da solução
- **Lower bound**: Melhor limite inferior conhecido
- **Gap de otimalidade**: Percentagem entre solução atual e bound
- **Estatísticas de busca**: Branches, conflicts

## 5. Execução da Resolução

### 5.1 Processo de Solve
```python
solve_start = time.time()
status = solver.Solve(model, solution_callback)
solve_end = time.time()
actual_duration = solve_end - solve_start
```

### 5.2 Análise de Status de Solução

#### **Status Possíveis**
- **`OPTIMAL`**: Solução ótima provada matematicamente
- **`FEASIBLE`**: Solução viável encontrada (pode não ser ótima)
- **`INFEASIBLE`**: Problema sem solução (constraints contraditórias)
- **`MODEL_INVALID`**: Modelo mal formado
- **`UNKNOWN`**: Timeout ou limite de recursos atingido

#### **Tratamento de Falhas**
```python
if status not in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
    if status == cp_model.INFEASIBLE:
        logger.error("Problem is infeasible - no solution exists with current constraints")
    elif status == cp_model.MODEL_INVALID:
        logger.error("Model is invalid - check constraint definitions")
    elif status == cp_model.UNKNOWN:
        logger.error("Solver timed out or encountered unknown status")
    
    raise RuntimeError(error_msg)
```

### 5.3 Estatísticas de Performance
```python
logger.info(f"Solver statistics:")
logger.info(f"  - Objective value: {solver.ObjectiveValue()}")
logger.info(f"  - Best objective bound: {solver.BestObjectiveBound()}")
logger.info(f"  - Number of branches: {solver.NumBranches()}")
logger.info(f"  - Number of conflicts: {solver.NumConflicts()}")
logger.info(f"  - Wall time: {solver.WallTime():.2f} seconds")
```

## 6. Processamento da Solução

### 6.1 Mapeamento de Turnos
```python
shift_mapping = {
    'M'     : 'M',  # Turno manhã (08:00-16:00)
    'T'     : 'T',  # Turno tarde (16:00-00:00)
    'F'     : 'F',  # Feriado encerrado
    'V'     : 'V',  # Dia vazio (colaborador não disponível)
    'A'     : 'A',  # Ausência (falta justificada/injustificada)
    'L'     : 'L',  # Folga normal
    'LQ'    : 'LQ', # Folga de qualidade (fim de semana)
    'TC'    : 'TC', # Turno especial (se aplicável)
    '-'     : '-'   # Não atribuído (erro de atribuição)
}
```

### 6.2 Extração de Atribuições
```python
for w in workers:
    worker_row = [w]
    l_count = lq_count = special_days_count = unassigned_days = 0
    
    for d in sorted(days_of_year):
        day_assignment = None
        
        # Verificar cada tipo de turno para este dia
        for s in shifts:
            if (w, d, s) in shift and solver.Value(shift[(w, d, s)]) == 1:
                day_assignment = shift_mapping.get(s, s)
                break
        
        # Contabilizar se não foi atribuído
        if day_assignment is None:
            day_assignment = '-'
            unassigned_days += 1
        
        worker_row.append(day_assignment)
        
        # Estatísticas por tipo de turno
        if day_assignment == 'L':
            l_count += 1
        elif day_assignment == 'LQ':
            lq_count += 1
        elif day_assignment in ['M', 'T'] and d in special_days:
            special_days_count += 1
```

### 6.3 Debugging de Variáveis Auxiliares
```python
if debug_vars:
    logger.info("Printing debug variables:")
    for var_name, var in debug_vars.items():
        try:
            var_value = solver.Value(var)
            logger.info(f"  {var_name} = {var_value}")
        except Exception as e:
            logger.warning(f"Could not read debug variable {var_name}: {e}")
```

## 7. Construção do DataFrame Final

### 7.1 Estrutura de Output
```python
columns = ['Worker'] + [f'Day_{d}' for d in sorted(days_of_year)]
df = pd.DataFrame(table_data, columns=columns)
```

**Características do DataFrame**:
- **Linhas**: Uma linha por colaborador
- **Colunas**: 'Worker' + uma coluna por dia do ano
- **Células**: Tipo de turno atribuído ou '-' se não atribuído
- **Formato**: Wide format (cada dia numa coluna separada)



### 7.2 Exportação para Excel
```python
try:
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    df.to_excel(output_filename, index=False)
    logger.info(f"Schedule saved to: {output_filename}")
except Exception as e:
    logger.warning(f"Could not save to Excel: {str(e)}")
```

## 8. Análise de Performance e Tuning

### 8.1 Métricas de Qualidade
- **Gap de otimalidade**: Distância entre solução encontrada e bound teórico
- **Tempo de primeira solução**: Rapidez para encontrar solução viável
- **Tempo total**: Eficiência global do processo
- **Número de branches**: Complexidade da busca
- **Número de conflicts**: Dificuldade de satisfação



O solver CP-SAT representa o estado da arte em resolução de problemas de constraint programming, utilizando técnicas avançadas de propagação, busca inteligente e otimização para encontrar soluções de alta qualidade para o complexo problema de scheduling do algoritmo SALSA.

---

# Classe `salsaAlgorithm`

A classe `SalsaAlgorithm` herda de `BaseAlgorithm` e implementa o padrão de três fases definido na arquitetura base. Esta classe é o ponto de entrada principal para a execução do algoritmo SALSA, coordenando todo o processo desde o processamento de dados até à formatação final dos resultados.

## 1. Arquitetura e Herança

### 1.1 Estrutura de Herança
```python
class SalsaAlgorithm(BaseAlgorithm):
    """
    SALSA shift scheduling algorithm implementation.

    This algorithm implements a constraint programming approach for shift scheduling:
    1. Adapt data: Read and process input DataFrames (calendario, estimativas, colaborador)
    2. Execute algorithm: Solve scheduling problem with SALSA-specific constraints
    3. Format results: Return final schedule DataFrame
    """
```

**Benefícios da herança**:
- **Padronização**: Interface consistente com outros algoritmos
- **Logging unificado**: Sistema de logging herdado da classe base
- **Gestão de parâmetros**: Estrutura comum para configuração
- **Tratamento de erros**: Mecanismos de erro standardizados

### 1.2 Inicialização Detalhada
```python
def __init__(self, parameters=None, algo_name: str = 'salsa_algorithm', 
             project_name: str = PROJECT_NAME, process_id: int = 0, 
             start_date: str = '', end_date: str = ''):
```

**Parâmetros de inicialização**:
- **`parameters`**: Dicionário com configurações específicas do algoritmo
- **`algo_name`**: Identificador único do algoritmo (padrão: 'salsa_algorithm')
- **`project_name`**: Nome do projeto (herdado de PROJECT_NAME)
- **`process_id`**: Identificador do processo de execução
- **`start_date`** e **`end_date`**: Período de escalonamento (formato: 'YYYY-MM-DD')

### 1.3 Parâmetros Default Detalhados
```python
default_parameters = {
    "max_continuous_working_days": 6,
    "shifts": ["M", "T", "L", "LQ", "F", "A", "V", "-"],
    "check_shifts": ['M', 'T', 'L', 'LQ'],
    "working_shifts": ["M", "T"],
    "settings": {
        "F_special_day": False,
        "free_sundays_plus_c2d": False,
        "missing_days_afect_free_days": False,
    }
}
```

**Explicação dos parâmetros**:
- **`max_continuous_working_days`**: Máximo de dias consecutivos de trabalho (limite legal: 6)
- **`shifts`**: Todos os tipos de turno/estado possíveis no sistema
- **`check_shifts`**: Turnos válidos para verificação de regras
- **`working_shifts`**: Apenas turnos de trabalho efetivo (excluindo folgas)
- **`settings`**: Configurações específicas do modelo SALSA

**Configurações especiais (`settings`)**:
- **`F_special_day`**: Se dias F (feriados) afetam cálculos de c2d e cxx
- **`free_sundays_plus_c2d`**: Se deve somar fins de semana de qualidade com folgas ao domingo
- **`missing_days_afect_free_days`**: Se dias em falta afetam cálculo de folgas

### 1.4 Atributos de Instância
```python
# Atributos de controlo de estado
self.data_processed = None      # Dados processados por adapt_data()
self.model = None              # Modelo CP-SAT criado
self.final_schedule = None     # DataFrame final com schedule
self.process_id = process_id   # ID do processo
self.start_date = start_date   # Data de início
self.end_date = end_date       # Data de fim
```

## 2. Método `adapt_data()` - Processamento de Entrada

### 2.1 Assinatura e Propósito
```python
def adapt_data(self, data: Dict[str, pd.DataFrame], 
               algorithm_treatment_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
```

**Objetivo**: Transformar dados brutos (DataFrames) em estruturas otimizadas para o algoritmo CP-SAT

### 2.2 Validação Rigorosa de Entrada
```python
# =================================================================
# 1. VALIDATE INPUT DATA STRUCTURE
# =================================================================
if data is None:
    raise ValueError("No data provided to adapt_data method. Expected dictionary with DataFrames.")

if not isinstance(data, dict):
    raise TypeError(f"Expected dictionary, got {type(data)}")
# Log da estrutura para debugging

self.logger.info(f"Input data structure: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
```

**Validações implementadas**:
- Verificação de tipo de dados (deve ser dicionário)
- Existência da chave 'medium_dataframes'
- Validação de estrutura interna dos DataFrames
- Logging detalhado para debugging

### 2.3 Extração e Processamento
```python
# Extract medium dataframes
if 'medium_dataframes' in data:
    medium_dataframes = data['medium_dataframes']
    self.logger.info("Found nested medium_dataframes structure")
else:
    medium_dataframes = data
    self.logger.info("Using direct DataFrame structure")

if not isinstance(medium_dataframes, dict):
    raise TypeError(f"Expected medium_dataframes to be dictionary, got {type(medium_dataframes)}")
```

### 2.4 Chamada ao Processador SALSA
```python
# Import e chamada da função de processamento SALSA
from src.algorithms.model_salsa.read_salsa import read_data_salsa

processed_data = read_data_salsa(medium_dataframes, algorithm_treatment_params)
```

**Função `read_data_salsa()`**:
- Processa os 3 DataFrames principais (calendario, estimativas, colaborador)
- Retorna tupla com 42 elementos estruturados
- Transforma dados relacionais em estruturas otimizadas para CP-SAT

### 2.5 Desempacotamento e Estruturação
```python
try:
    # Criar dicionário estruturado
    data_dict = {
        'matriz_calendario_gd': matriz_calendario_gd,
        'days_of_year': days_of_year,
        # ... todos os 42 elementos ...
    }

except IndexError as e:
    self.logger.error(f"Error unpacking processed data: {e}")
    raise ValueError(f"Invalid data structure returned from processing function: {e}")
```

### 2.6 Validação Final e Estatísticas
```python
    workers = data_dict['workers']
    days_of_year = data_dict['days_of_year']
    special_days = data_dict['special_days']
    working_days = data_dict['working_days']
    # Validate critical data
    if not workers:
        raise ValueError("No valid workers found after processing")

    if not days_of_year:
        raise ValueError("No valid days found after processing")

    # Log final statistics
    self.logger.info("[OK] Data adaptation completed successfully")
    self.logger.info(f"[STATS] Final statistics:")
    self.logger.info(f"   Total workers: {len(workers)}")
    self.logger.info(f"   Total days: {len(days_of_year)}")
    self.logger.info(f"   Working days: {len(working_days)}")
    self.logger.info(f"   Special days: {len(special_days)}")
    self.logger.info(f"   Week mappings: {len(data_dict['week_to_days'])}")

    # Store processed data in instance
    self.data_processed = data_dict

    return data_dict
except Exception as e:
    self.logger.error(f"Error in data adaptation: {e}", exc_info=True)
    raise
```

## 3. Método `execute_algorithm()` - Coração da Otimização

### 3.1 Assinatura e Validação Inicial
```python
def execute_algorithm(self, adapted_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute the SALSA shift scheduling algorithm.
    
    Args:
        adapted_data: Processed data from adapt_data method
        
    Returns:
        Final schedule DataFrame
    """
    try:
        self.logger.info("Starting SALSA algorithm execution")
        
        if adapted_data is None:
            adapted_data = self.data_processed
```

### 3.2 Extração Massiva de Variáveis
```python
# Extração de TODAS as 42 variáveis processadas
matriz_calendario_gd = adapted_data['matriz_calendario_gd']
days_of_year = adapted_data['days_of_year']
sundays = adapted_data['sundays']
holidays = adapted_data['holidays']
special_days = adapted_data['special_days']
closed_holidays = adapted_data['closed_holidays']
empty_days = adapted_data['empty_days']
worker_holiday = adapted_data['worker_holiday']
missing_days = adapted_data['missing_days']
working_days = adapted_data['working_days']
non_holidays = adapted_data['non_holidays']
start_weekday = adapted_data['start_weekday']
week_to_days = adapted_data['week_to_days']
worker_week_shift = adapted_data['worker_week_shift']
matriz_colaborador_gd = adapted_data['matriz_colaborador_gd']
workers = adapted_data['workers']
contract_type = adapted_data['contract_type']
total_l = adapted_data['total_l']
total_l_dom = adapted_data['total_l_dom']
c2d = adapted_data['c2d']
c3d = adapted_data['c3d']
l_d = adapted_data['l_d']
l_q = adapted_data['l_q']
cxx = adapted_data['cxx']
t_lq = adapted_data['t_lq']
matriz_estimativas_gd = adapted_data['matriz_estimativas_gd']
pessObj = adapted_data['pess_obj']
min_workers = adapted_data['min_workers']
max_workers = adapted_data['max_workers']
workers_complete = adapted_data['workers_complete']
workers_complete_cycle = adapted_data['workers_complete_cycle']
free_day_complete_cycle = adapted_data['free_day_complete_cycle']
week_to_days_salsa = adapted_data['week_to_days_salsa']
first_day = adapted_data['first_registered_day']
admissao_proporcional = adapted_data['admissao_proporcional']
data_admissao = adapted_data['data_admissao']
data_demissao = adapted_data['data_demissao']
last_day = adapted_data['last_registered_day']
fixed_days_off = adapted_data['fixed_days_off']
fixed_LQs = adapted_data['fixed_LQs']
role_by_worker = adapted_data['role_by_worker']
proportion = adapted_data['proportion']

# Extração de parâmetros do algoritmo
shifts = self.parameters["shifts"]
check_shift = self.parameters["check_shifts"]
working_shift = self.parameters["working_shifts"]
max_continuous_days = self.parameters["max_continuous_working_days"]

# Extração de configurações especiais
settings = self.parameters["settings"]
F_special_day = settings["F_special_day"]
free_sundays_plus_c2d = settings["free_sundays_plus_c2d"]
missing_days_afect_free_days = settings["missing_days_afect_free_days"]
```

### 3.3 Criação do Modelo CP-SAT
```python
# =================================================================
# CREATE MODEL AND DECISION VARIABLES
# =================================================================
self.logger.info("Creating SALSA model and decision variables")

model = cp_model.CpModel()
self.model = model  # Armazenar para acesso posterior

logger.info(f"workers_complete: {workers_complete}")
```

### 3.4 Fase 1: Criação de Variáveis de Decisão
```python
# Criar variáveis de decisão usando função especializada
shift = decision_variables(
    model=model,
    days_of_year=days_of_year,
    workers=workers_complete,
    shifts=shifts,
    first_day=first_day,
    last_day=last_day,
    absences=worker_holiday,
    missing_days=missing_days,
    empty_days=empty_days,
    closed_holidays=closed_holidays,
    fixed_days_off=fixed_days_off,
    fixed_LQs=fixed_LQs,
    start_weekday=start_weekday
)

self.logger.info("Decision variables created for SALSA")
```

### 3.5 Fase 2: Aplicação de Restrições
```python
# =================================================================
# APPLY ALL CONSTRAINTS
# =================================================================
self.logger.info("Applying SALSA constraints")


shift_day_constraint(model, shift, days_of_year, workers_complete, shifts)
week_working_days_constraint(model, shift, week_to_days, workers, working_shift, contract_type)
maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)


LQ_attribution(model, shift, workers, working_days, c2d)
assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift)


working_day_shifts(model, shift, workers, working_days, check_shift, workers_complete_cycle, working_shift)

salsa_2_consecutive_free_days(model, shift, workers, working_days)
salsa_2_day_quality_weekend(model, shift, workers, contract_type, working_days, sundays, c2d, F_special_day, days_of_year, closed_holidays)
salsa_saturday_L_constraint(model, shift, workers, working_days, start_weekday, days_of_year, non_holidays)
salsa_2_free_days_week(model, shift, workers, week_to_days_salsa, working_days, contract_type, admissao_proporcional, data_admissao, data_demissao, fixed_days_off, fixed_LQs)


first_day_not_free(model, shift, workers, working_days, first_day, working_shift)
free_days_special_days(model, shift, sundays, workers, working_days, total_l_dom)

self.logger.info("All constraints applied successfully")
```

### 3.6 Fase 3: Definição da Função Objetivo
```python
# =================================================================
# DEFINE OBJECTIVE FUNCTION
# =================================================================
self.logger.info("Defining SALSA optimization objective")

debug_vars = salsa_optimization(
    model=model,
    shift=shift,
    days_of_year=days_of_year,
    workers=workers,
    working_days=working_days,
    working_shift=working_shift,
    pessObj=pessObj,
    min_workers=min_workers,
    sundays=sundays,
    week_to_days=week_to_days,
    c2d=c2d,
    closed_holidays=closed_holidays,
    role_by_worker=role_by_worker
)

self.logger.info("Objective function defined successfully")
```

### 3.7 Fase 4: Resolução
```python
# =================================================================
# SOLVE THE MODEL
# =================================================================
self.logger.info("Calling CP-SAT solver")

final_schedule = solve(
    model=model,
    days_of_year=days_of_year,
    workers=workers_complete,
    special_days=special_days,
    shift=shift,
    shifts=shifts,
    max_time_seconds=600,
    enumerate_all_solutions=False,
    use_phase_saving=True,
    log_search_progress=0,
    output_filename=os.path.join(ROOT_DIR, 'data', 'output', 'salsa_schedule.xlsx'),
    debug_vars=debug_vars
)

# Armazenar resultado
self.final_schedule = final_schedule
self.logger.info(f"Algorithm execution completed. Schedule shape: {final_schedule.shape}")

return final_schedule
```

## 4. Método `format_results()` - Formatação de Saída

### 4.1 Estrutura Avançada de Formatação
```python
def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame()) -> Dict[str, Any]:
    """
    Format the SALSA algorithm results for output.
    
    Returns:
        Dictionary containing comprehensive formatted results and metadata
    """
    try:
        self.logger.info("Formatting SALSA algorithm results")

        # Usar schedule armazenado se results estiver vazio
        if algorithm_results.empty and self.final_schedule is not None:
            algorithm_results = self.final_schedule

        # Usar funções helper para formatação avançada
        if algorithm_results.empty:
            return _create_empty_results(self.algo_name, self.process_id, 
                                       self.start_date, self.end_date, self.parameters)

        # Calcular estatísticas abrangentes
        comprehensive_stats = _calculate_comprehensive_stats(
            algorithm_results, self.start_date, self.end_date, self.data_processed
        )

        # Validar restrições
        constraint_validation = _validate_constraints(algorithm_results)

        # Calcular métricas de qualidade
        quality_metrics = _calculate_quality_metrics(algorithm_results)

        # Formatar schedules em múltiplos formatos
        formatted_schedules = _format_schedules(algorithm_results, self.start_date, self.end_date)

        # Atributos do solver (se disponível)
        solver_attributes = {}
        if hasattr(self, 'model') and self.model is not None:
            solver_attributes = {
                'model_variables': self.model.Proto().variables,
                'model_constraints': len(self.model.Proto().constraints),
            }

        # Criar metadata completa
        metadata = _create_metadata(
            self.algo_name, self.process_id, self.start_date, self.end_date,
            self.parameters, comprehensive_stats, solver_attributes
        )

        # Validação final da solução
        solution_validation = _validate_solution(algorithm_results)

        # Informações de exportação
        export_info = _create_export_info(self.process_id, ROOT_DIR)

        # Resultado final estruturado
        formatted_results = {
            'schedule': algorithm_results,
            'metadata': metadata,
            'comprehensive_stats': comprehensive_stats,
            'constraint_validation': constraint_validation,
            'quality_metrics': quality_metrics,
            'formatted_schedules': formatted_schedules,
            'solution_validation': solution_validation,
            'export_info': export_info,
            'summary': {
                'status': 'completed',
                'total_workers': comprehensive_stats['total_workers'],
                'total_days': comprehensive_stats['total_days'],
                'execution_timestamp': datetime.now().isoformat(),
                'message': f'SALSA algorithm executed successfully: {comprehensive_stats["total_assignments"]} assignments'
            }
        }

        self.logger.info(f"Results formatted successfully: {comprehensive_stats['total_assignments']} total assignments")
        return formatted_results

    except Exception as e:
        self.logger.error(f"Error in SALSA results formatting: {e}", exc_info=True)
        raise
```


## 5. Características Especiais

### 5.1 Logging Detalhado
- **Validações de entrada**: Verificação rigorosa de tipos e estruturas
- **Estatísticas de processamento**: Contadores de workers, dias, restrições
- **Progresso de otimização**: Estado do solver em tempo real
- **Métricas de qualidade**: Análise da solução final

### 5.2 Tratamento Robusto de Erros
- **Dados em falta**: Validação de DataFrames obrigatórios
- **Modelos infeasíveis**: Detecção e reporting de problemas
- **Timeouts de solver**: Gestão de limites de tempo


### 5.3 Flexibilidade de Configuração
- **Parâmetros dinâmicos**: Configuração via dicionário de parâmetros
- **Settings específicos**: Flags para comportamentos especiais do SALSA
- **Períodos customizáveis**: Suporte para qualquer intervalo de datas
- **Debugging avançado**: Variáveis auxiliares para análise

Esta arquitetura modular e robusta permite execução confiável do algoritmo SALSA em ambiente de produção, mantendo flexibilidade para customização e debugging durante desenvolvimento.



Esta arquitetura modular permite fácil manutenção, teste e extensão do algoritmo SALSA, mantendo separação clara entre processamento de dados, modelação matemática e apresentação de resultados.

# Solver e Execução

O algoritmo SALSA utiliza o solver CP-SAT (Constraint Programming - Satisfiability) do OR-Tools, um dos solvers de constraints mais avançados disponíveis. Esta secção detalha como o solver é configurado, como processa a resolução, e como extrai e formata os resultados.



### Estratégias de Branch-and-Bound
```python
# O CP-SAT usa branch-and-bound inteligente:

# 1. Variable Selection Heuristics
# - Escolhe variáveis com maior impacto na função objectivo
# - Prioriza variáveis mais restringidas
# - Usa heurísticas específicas para scheduling

# 2. Value Selection Heuristics  
# - Para variáveis booleanas: tenta primeiro valor que reduz espaço de busca
# - Para variáveis inteiras: tenta valores centrais do domínio
# - Usa informação da função objectivo para guiar escolhas

# 3. Constraint Propagation
# - After each variable assignment:
#   * Propaga implicações para outras variáveis
#   * Detecta inconsistências precocemente
#   * Reduz domínios das variáveis restantes

# 4. Backtracking Strategies
# - Conflict-driven backjumping
# - Nogood learning para evitar repetir falhas
# - Restart strategies para escapar de regiões ruins
```


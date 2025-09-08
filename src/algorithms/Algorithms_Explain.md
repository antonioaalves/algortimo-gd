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

Esta matriz representa as necessidades para um respetivo dia, num posto e para os diferentes tipos de turno (Manhã ou Tarde). Vai ser o principal 

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

data,media_turno,max_turno,min_turno,sd_turno,turno,fk_tipo_posto,data_turno,+H,aux,pess_obj,diff,WDAY

Correção das colunas de `df_estimativas`:

1. `data`: data específica da estimativa
2. `turno`: tipo de turno (M - Manhã, T - Tarde)
3. `media_turno`: número médio de pessoas necessárias para o turno
4. `max_turno`: número máximo de pessoas necessárias para o turno
5. `min_turno`: número mínimo de pessoas necessárias para o turno
6. `pess_obj`: número objetivo de pessoas para o turno (valor utilizado na otimização)
7. `sd_turno`: desvio padrão das necessidades do turno
8. `fk_tipo_posto`: identificador do tipo de posto
9. `wday`: dia da semana (1-7)

---

## 3. `df_calendario`

Esta matriz contém o histórico e planeamento base de cada colaborador, definindo a sua disponibilidade, tipo de turno já atribuído, e dias especiais. É a matriz que conecta colaboradores com dias específicos e serve como base para validar restrições e pré-atribuições.

As colunas são as seguintes:

1. `colaborador`: identificador do colaborador (correspondente à matricula)
2. `data`: data específica do registo
3. `wd`: dia da semana (Mon, Tue, Wed, Thu, Fri, Sat, Sun)
4. `dia_tipo`: classificação do tipo de dia (normal, domYf - domingo/feriado)
5. `tipo_turno`: turno já atribuído ou estado do colaborador:
   - `M`: turno da manhã
   - `T`: turno da tarde
   - `L`: dia de folga normal
   - `L_DOM`: dia de folga em domingo
   - `A/AP`: ausência/falta
   - `V`: dia vazio (não trabalha)
   - `F`: feriado encerrado
   - `-`: dia não definido
6. `ww`: número da semana no ano (1-52/53)

---

## 4. `algorithm_treatment_params`

Dicionário contendo parâmetros de configuração do algoritmo:

- `admissao_proporcional`: booleano que define se deve aplicar cálculo proporcional de folgas para colaboradores admitidos durante o período

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
required_colaborador_cols = ['matricula', 'l_total', 'l_dom', 'c2d', 'c3d', 'l_d', 'cxx', 'vz', 'data_admissao', 'data_demissao', 'l_dom_salsa', 'l_res', 'l_res2']
```
- `matricula`: identificador único do colaborador (chave primária)
- `l_total`: total de folgas contratuais anuais
- `l_dom`: folgas obrigatórias em domingos/feriados
- `c2d`: fins de semana de qualidade (sábado+domingo)
- `c3d`: fins de semana longos (sexta+sábado+domingo ou sábado+domingo+segunda)
- `l_d`: folgas compensatórias por trabalho em dias especiais
- `cxx`: dias de folga consecutivos permitidos
- `vz`: dias de folga adicionais específicos
- `data_admissao/data_demissao`: datas contratuais
- `l_dom_salsa`: folgas mínimas em domingos para o contexto SALSA
- `l_res/l_res2`: folgas reservadas para situações especiais

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

**Lógica de cálculo**:
- **l_total**: total de folgas contratuais (base)
- **Subtraem-se folgas já atribuídas ou reservadas**:
  - `l_dom`: folgas obrigatórias em domingos
  - `c2d`: fins de semana de qualidade
  - `c3d`: fins de semana longos
  - `l_d`: compensações por trabalho em dias especiais
  - `cxx`: folgas consecutivas
  - `vz`: folgas adicionais
  - `l_res + l_res2`: folgas reservadas
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

## 4. Extração de Informação Temporal Detalhada

### 4.1 Extração de Dias do Ano
```python
days_of_year = sorted(matriz_calendario_gd['data'].dt.dayofyear.unique().tolist())
```
**Processo**:
- Converte datas para dia do ano (1-365/366)
- Remove duplicados e ordena cronologicamente
- Cria base temporal para indexação do modelo CP

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

### 4.3 Construção da Estrutura Semanal
```python
# Criação de week_to_days
week_to_days = {}
for day in days_of_year:
    # Calcular semana baseada no start_weekday
    week_number = calculate_week(day, start_weekday)
    if week_number not in week_to_days:
        week_to_days[week_number] = []
    week_to_days[week_number].append(day)

# Criação de week_to_days_salsa (segunda a sexta)
week_to_days_salsa = {}
for week, days in week_to_days.items():
    week_to_days_salsa[week] = [d for d in days if is_weekday(d, start_weekday)]
```

**Estruturas criadas**:
- **week_to_days**: mapeamento completo semana → todos os dias
- **week_to_days_salsa**: mapeamento semana → apenas dias úteis (seg-sex)
- **start_weekday**: dia da semana do primeiro dia (para cálculos correctos)

## 5. Processamento Detalhado por Colaborador

### 5.1 Extração de Datas Contratuais
```python
for w in workers_complete:
    worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
    
    # Admissão
    admissao_value = worker_data['data_admissao'].iloc[0] if not worker_data.empty else None
    if admissao_value is not None and not pd.isna(admissao_value):
        data_admissao[w] = pd.to_datetime(admissao_value).dayofyear
    else:
        data_admissao[w] = 1  # Primeiro dia do período
    
    # Demissão
    demissao_value = worker_data['data_demissao'].iloc[0] if not worker_data.empty else None
    if demissao_value is not None and not pd.isna(demissao_value):
        data_demissao[w] = pd.to_datetime(demissao_value).dayofyear
    else:
        data_demissao[w] = max(days_of_year)  # Último dia do período
```

### 5.2 Categorização Detalhada de Dias por Colaborador
```python
worker_calendar = matriz_calendario_gd[matriz_calendario_gd['colaborador'] == w]

# Extração baseada em tipo_turno
worker_empty = worker_calendar[worker_calendar['tipo_turno'] == '-']['data'].dt.dayofyear.tolist()
worker_missing = worker_calendar[worker_calendar['tipo_turno'] == 'V']['data'].dt.dayofyear.tolist()
w_holiday = worker_calendar[(worker_calendar['tipo_turno'] == 'A') | (worker_calendar['tipo_turno'] == 'AP')]['data'].dt.dayofyear.tolist()
worker_fixed_days_off = worker_calendar[worker_calendar['tipo_turno'] == 'L']['data'].dt.dayofyear.tolist()
f_day_complete_cycle = worker_calendar[worker_calendar['tipo_turno'].isin(['L', 'L_DOM'])]['data'].dt.dayofyear.tolist()
```

**Significado de cada categoria**:
- **empty_days** (`tipo_turno == '-'`): dias onde o colaborador não está disponível
- **missing_days** (`tipo_turno == 'V'`): dias de ausência/vazio
- **worker_holiday** (`tipo_turno == 'A'/'AP'`): ausências justificadas
- **fixed_days_off** (`tipo_turno == 'L'`): folgas já pré-atribuídas
- **fixed_LQs**: fins de semana de qualidade já determinados
- **free_day_complete_cycle**: folgas para trabalhadores de ciclo completo

### 5.3 Rastreamento de Primeiro e Último Dia
```python
# Primeiro dia registado
if w in matriz_calendario_gd['colaborador'].values:
    worker_calendar = matriz_calendario_gd[matriz_calendario_gd['colaborador'] == w]
    first_registered_day[w] = worker_calendar['data'].dt.dayofyear.min()
else:
    first_registered_day[w] = data_admissao.get(w, 1)

# Último dia registado
if w in matriz_calendario_gd['colaborador'].values:
    last_registered_day[w] = worker_calendar['data'].dt.dayofyear.max()
else:
    last_registered_day[w] = data_demissao.get(w, max(days_of_year))
```

### 5.4 Tratamento de Ausências e Limpeza
```python
# Marcar dias antes da admissão como missing
if first_registered_day[w] > 0 or last_registered_day[w] > 0:
    missing_days[w].extend([d for d in range(1, first_registered_day[w]) if d not in missing_days[w]])
    missing_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in missing_days[w]])

# Remover feriados encerrados de todas as categorias
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
```python
if not working_days[w]:
    logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")
```
- Detecta colaboradores sem dias válidos (possível erro de dados)
- Gera warnings para investigação manual
- Previne falhas no modelo CP (variáveis sem domínio)

---

# Criação das Variáveis

O modelo de programação por restrições utiliza um sistema sofisticado de variáveis booleanas para representar todas as decisões de atribuição de turnos. Esta secção detalha cada aspecto da criação e estruturação dessas variáveis.

## 1. Arquitectura da Variável Principal: `shift`

### 1.1 Estrutura Tridimensional
```python
shift[(worker, day, shift_type)] → cp_model.IntVar
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
def decision_variables(workers_complete, days_of_year, shifts, working_days, 
                      worker_holiday, missing_days, empty_days, closed_holidays,
                      fixed_days_off, fixed_LQs, first_day, last_day, start_weekday):
    
    model = cp_model.CpModel()
    shift = {}
    
    for w in workers_complete:
        for d in days_of_year:
            # Verificação de elegibilidade temporal
            if d < first_day.get(w, 1) or d > last_day.get(w, max(days_of_year)):
                continue  # Colaborador não activo neste dia
                
            # Verificação de exclusões específicas
            if (d in worker_holiday.get(w, []) or 
                d in missing_days.get(w, []) or 
                d in empty_days.get(w, []) or 
                d in closed_holidays or
                d in fixed_days_off.get(w, []) or
                d in fixed_LQs.get(w, [])):
                continue  # Dia já pré-determinado
                
            for s in shifts:
                # Verificação de compatibilidade turno-dia
                if is_shift_compatible(w, d, s, start_weekday):
                    var_name = f"shift_w{w}_d{d}_s{s}"
                    shift[(w, d, s)] = model.NewBoolVar(var_name)
    
    return model, shift
```

**Processo de filtragem**:
1. **Filtragem temporal**: só cria variáveis entre first_day[w] e last_day[w]
2. **Filtragem por exclusões**: remove dias já pré-determinados
3. **Filtragem por compatibilidade**: verifica se turno é válido para o dia da semana
4. **Nomeação sistemática**: cada variável tem nome único para debug

### 1.3 Tipos de Turnos e Suas Implicações

#### Turnos de Trabalho
- **"M" (Manhã)**: 
  - Horário típico: 06:00-14:00
  - Compatível com: dias úteis, alguns fins de semana
  - Incompatível com: feriados encerrados
  
- **"T" (Tarde)**: 
  - Horário típico: 14:00-22:00
  - Compatível com: dias úteis, alguns fins de semana
  - Incompatível com: feriados encerrados

#### Estados de Não-Trabalho
- **"L" (Folga normal)**: 
  - Dia de descanso regular
  - Conta para quotas de folgas semanais
  - Pode ser consecutivo (com limites)
  
- **"LQ" (Folga de qualidade)**: 
  - Folga em fim de semana de qualidade
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

### 2.1 Limitação Temporal Rigorosa
```python
if d < first_day.get(w, 1) or d > last_day.get(w, max(days_of_year)):
    continue  # Não cria variável
```

**Lógica detalhada**:
- **first_day[w]**: primeiro dia onde colaborador aparece no calendário OU data de admissão
- **last_day[w]**: último dia onde colaborador aparece no calendário OU data de demissão
- **Não criação**: economiza memória e simplifica o modelo
- **Validação**: previne atribuições impossíveis

### 2.2 Exclusão de Dias Pré-determinados
```python
exclusion_sets = [
    worker_holiday.get(w, []),      # Ausências confirmadas
    missing_days.get(w, []),        # Dias vazios
    empty_days.get(w, []),          # Indisponibilidades
    closed_holidays,                # Encerramentos gerais
    fixed_days_off.get(w, []),      # Folgas pré-atribuídas
    fixed_LQs.get(w, [])           # LQs pré-determinados
]

if d in any(exclusion_sets):
    continue  # Não cria variável para este dia
```

**Impacto na optimização**:
- **Redução do espaço de busca**: menos variáveis = menos tempo de resolução
- **Garantia de consistência**: pré-atribuições são respeitadas automaticamente
- **Prevenção de conflitos**: evita situações logicamente impossíveis

### 2.3 Validação de Compatibilidade Turno-Dia
```python
def is_shift_compatible(worker, day, shift_type, start_weekday):
    day_of_week = (day + start_weekday - 2) % 7  # 0=Mon, 6=Sun
    
    # Regras de compatibilidade
    if shift_type in ["M", "T"]:  # Turnos de trabalho
        return True  # Trabalho é possível qualquer dia (exceto exclusões)
    
    elif shift_type == "LQ":  # Folga de qualidade
        return day_of_week == 5  # Apenas sábados podem ter LQ
    
    elif shift_type == "L":   # Folga normal
        return True  # Folga é possível qualquer dia
    
    elif shift_type == "F":   # Feriado encerrado
        return day in closed_holidays  # Apenas em dias de encerramento
    
    return False  # Outros tipos não são atribuíveis dinamicamente
```

### 2.4 Considerações de Memória e Performance
**Estimativa de variáveis**:
```
Total variáveis ≈ workers × days × shifts × eligibility_rate
Exemplo: 50 workers × 365 days × 8 shifts × 0.7 = ~102,200 variáveis
```

**Optimizações implementadas**:
- **Lazy creation**: só cria variáveis quando necessário
- **Sparse representation**: usa dicionários em vez de arrays
- **Naming convention**: nomes sistemáticos para debug eficiente
- **Memory profiling**: monitorização do uso de memória

## 3. Estruturas de Apoio às Variáveis

### 3.1 Mapeamentos de Suporte
```python
# Mapeamento inverso: dia → colaboradores activos
day_to_workers = {}
for (w, d, s), var in shift.items():
    if d not in day_to_workers:
        day_to_workers[d] = set()
    day_to_workers[d].add(w)

# Mapeamento inverso: colaborador → dias activos
worker_to_days = {}
for (w, d, s), var in shift.items():
    if w not in worker_to_days:
        worker_to_days[w] = set()
    worker_to_days[w].add(d)
```

### 3.2 Indexação para Restrições
```python
# Agrupamento por semana para restrições semanais
week_variables = {}
for week, days in week_to_days.items():
    week_variables[week] = {}
    for w in workers:
        week_variables[week][w] = []
        for d in days:
            for s in ["M", "T"]:  # Apenas turnos de trabalho
                if (w, d, s) in shift:
                    week_variables[week][w].append(shift[(w, d, s)])
```

**Utilização nas restrições**:
- **Restrições semanais**: acesso directo a variáveis da semana
- **Restrições de consistência**: validação cruzada de atribuições
- **Optimização de objectivos**: cálculos agregados eficientes

## 4. Validação e Debugging das Variáveis

### 4.1 Verificações de Integridade
```python
def validate_variables(shift, workers_complete, days_of_year, shifts):
    # Verificar completude
    expected_combinations = 0
    actual_combinations = len(shift)
    
    for w in workers_complete:
        for d in days_of_year:
            if is_day_eligible(w, d):
                for s in shifts:
                    if is_shift_compatible(w, d, s):
                        expected_combinations += 1
    
    logger.info(f"Variable coverage: {actual_combinations}/{expected_combinations} ({100*actual_combinations/expected_combinations:.1f}%)")
    
    # Verificar consistência
    for (w, d, s), var in shift.items():
        assert w in workers_complete, f"Invalid worker {w}"
        assert d in days_of_year, f"Invalid day {d}"
        assert s in shifts, f"Invalid shift {s}"
        assert var.name.startswith(f"shift_w{w}_d{d}_s{s}"), f"Inconsistent naming {var.name}"
```

### 4.2 Estatísticas de Criação
```python
def print_variable_statistics(shift):
    # Contagem por tipo
    shift_counts = {}
    for (w, d, s), var in shift.items():
        if s not in shift_counts:
            shift_counts[s] = 0
        shift_counts[s] += 1
    
    # Contagem por colaborador
    worker_counts = {}
    for (w, d, s), var in shift.items():
        if w not in worker_counts:
            worker_counts[w] = 0
        worker_counts[w] += 1
    
    logger.info(f"Variables by shift type: {shift_counts}")
    logger.info(f"Variables per worker: min={min(worker_counts.values())}, max={max(worker_counts.values())}, avg={sum(worker_counts.values())/len(worker_counts):.1f}")
```

Este sistema de variáveis forma a base matemática sobre a qual todas as restrições e objectivos são construídos, garantindo que o modelo CP-SAT pode explorar eficientemente o espaço de soluções viáveis.

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
Dias 100-107 (janela de 8 dias):
Se trabalha 100,101,102,103,104,105,106: 7 ≤ 6 ✗ (violação)
Se trabalha 100,101,102,103,104,105,107: 7 ≤ 6 ✗ (violação) 
Se trabalha 100,101,102,103,104,107: 6 ≤ 6 ✓
```

**Benefícios para bem-estar**:
- **Prevenção de burnout**: força períodos de descanso
- **Compliance regulamentar**: respeita leis laborais
- **Distribuição equilibrada**: evita concentração excessiva de trabalho

## 2. Restrições de Atribuição Obrigatória

### 2.1 `closed_holiday_attribution()` - Folgas Forçadas em Feriados
```python
def closed_holiday_attribution(model, shift, workers_complete, closed_holidays):
    for w in workers_complete:
        for d in closed_holidays:
            if (w, d, "F") in shift:
                model.Add(shift[(w, d, "F")] == 1)
            else:
                logger.info(f"Missing shift for worker {w}, day {d}, shift F")
```

**Semântica**:
- **Atribuição forçada**: `shift[(w, d, "F")] = 1` (não negociável)
- **Aplicação universal**: todos os colaboradores afectados
- **Logging de inconsistências**: detecta problemas de configuração

**Implicações operacionais**:
- **Encerramento geral**: loja/empresa fechada
- **Não contabilização**: não conta para limites de trabalho ou folgas
- **Simplificação do modelo**: reduz escolhas do solver

### 2.2 `holiday_missing_day_attribution()` - Pré-atribuições do Calendário
```python
def holiday_missing_day_attribution(model, shift, workers_complete, worker_holiday, 
                                  missing_days, empty_days, free_day_complete_cycle):
    for w in workers_complete:
        # Dias vazios → V
        for d in empty_days[w]:
            if (w, d, "V") in shift:
                model.Add(shift[(w, d, "V")] == 1)
        
        # Folgas de ciclo completo → L
        for d in free_day_complete_cycle[w]:
            if (w, d, "L") in shift:
                model.Add(shift[(w, d, "L")] == 1)
```

**Categorias de pré-atribuição**:
- **empty_days**: colaborador indisponível (atribuição → "V")
- **free_day_complete_cycle**: folgas predefinidas para ciclo completo (atribuição → "L")
- **worker_holiday**: ausências confirmadas (atribuição → "A") [comentado no código]
- **missing_days**: faltas/vazios (atribuição → "V") [comentado no código]

### 2.3 `LQ_attribution()` - Quota Mínima de Fins de Semana de Qualidade
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
- **c2d**: "Two-day weekends" - fins de semana de qualidade contratuais
- **Quota mínima**: garantia contratual de fins de semana livres
- **Flexibilidade de timing**: algoritmo escolhe quando atribuir

## 3. Restrições de Turnos de Trabalho

### 3.1 `assign_week_shift()` - Padrão Semanal de Turnos
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
- **Consistência semanal**: colaborador trabalha sempre o mesmo turno numa semana
- **Prevenção de mixing**: evita manhã segunda + tarde terça na mesma semana
- **Simplificação operacional**: facilita gestão de equipas

### 3.2 `working_day_shifts()` - Turnos Válidos em Dias de Trabalho
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

## 4. Restrições Específicas SALSA

### 4.1 `salsa_2_consecutive_free_days()` - Limite de Folgas Consecutivas
```python
def salsa_2_consecutive_free_days(model, shift, workers, working_days):
    for w in workers:
        all_work_days = sorted(working_days[w])
        
        # Criar variáveis auxiliares para dias de folga
        free_day_vars = {}
        for d in all_work_days:
            free_day = model.NewBoolVar(f"free_day_{w}_{d}")
            
            free_shift_sum = sum(
                shift.get((w, d, shift_type), 0) 
                for shift_type in ["L", "F", "LQ"]
            )
            
            # Ligar variável auxiliar ao estado de folga
            model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
            model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())
            
            free_day_vars[d] = free_day
        
        # Aplicar restrição de 3 dias consecutivos
        for i in range(len(all_work_days) - 2):
            day1, day2, day3 = all_work_days[i:i+3]
            
            if day2 == day1 + 1 and day3 == day2 + 1:
                # Pelo menos um dos 3 dias não pode ser folga
                model.Add(free_day_vars[day1] + free_day_vars[day2] + free_day_vars[day3] <= 2)
```

**Lógica de prevenção**:
- **Variáveis auxiliares**: `free_day_vars[d]` = 1 se d é folga (L, F, ou LQ)
- **Triplets consecutivos**: verifica janelas de 3 dias seguidos
- **Regra dos 2-em-3**: máximo 2 folgas em qualquer triplet consecutivo
- **Benefício**: evita períodos excessivamente longos sem trabalho

### 4.2 `salsa_2_day_quality_weekend()` - Fins de Semana de Qualidade
```python
def salsa_2_day_quality_weekend(model, shift, workers, contract_type, working_days, 
                               sundays, c2d, F_special_day, days_of_year, closed_holidays):
    debug_vars = {}
    
    for w in workers:
        if contract_type[w] in [4, 5, 6]:
            quality_2weekend_vars = []
            
            # Para cada domingo, verificar se forma fim de semana de qualidade
            for sunday in sundays:
                if sunday in working_days[w]:
                    saturday = sunday - 1
                    
                    if saturday in days_of_year and saturday in working_days[w]:
                        # Criar variável para fim de semana de qualidade
                        could_be_quality = model.NewBoolVar(f"quality_weekend_w{w}_sun{sunday}")
                        
                        # Condições para fim de semana de qualidade
                        if F_special_day:
                            # LQ no sábado E (L ou F) no domingo
                            saturday_lq = shift.get((w, saturday, "LQ"), 0)
                            sunday_free = (shift.get((w, sunday, "L"), 0) + 
                                         shift.get((w, sunday, "F"), 0))
                            
                            model.Add(saturday_lq + sunday_free >= 2).OnlyEnforceIf(could_be_quality)
                            model.Add(saturday_lq + sunday_free <= 1).OnlyEnforceIf(could_be_quality.Not())
                        else:
                            # LQ no sábado E L no domingo
                            saturday_lq = shift.get((w, saturday, "LQ"), 0)
                            sunday_l = shift.get((w, sunday, "L"), 0)
                            
                            model.Add(saturday_lq + sunday_l >= 2).OnlyEnforceIf(could_be_quality)
                            model.Add(saturday_lq + sunday_l <= 1).OnlyEnforceIf(could_be_quality.Not())
                        
                        quality_2weekend_vars.append(could_be_quality)
                        debug_vars[f"quality_weekend_w{w}_sun{sunday}"] = could_be_quality
            
            # Garantir quota mínima de fins de semana de qualidade
            if quality_2weekend_vars:
                model.Add(sum(quality_2weekend_vars) >= c2d.get(w, 0))
    
    return debug_vars
```

**Definição de fim de semana de qualidade**:
- **Padrão base**: sábado com LQ + domingo com L
- **Variante com F_special_day**: sábado com LQ + domingo com (L ou F)
- **Aplicação restrita**: apenas contratos tipo 4, 5, 6

### 4.3 `salsa_saturday_L_constraint()` - Coordenação Sábado-Domingo
```python
def salsa_saturday_L_constraint(model, shift, workers, working_days, start_weekday, 
                               days_of_year, non_working_days):
    for w in workers:
        for day in working_days[w]:
            day_of_week = (day + start_weekday - 2) % 7  # 0=Mon, 6=Sun
            
            if day_of_week == 5:  # Sábado
                sunday = day + 1
                if sunday in working_days[w] and sunday in days_of_year:
                    # Se domingo tem L, então sábado também deve ter L
                    if (w, sunday, "L") in shift and (w, day, "L") in shift:
                        model.Add(shift[(w, day, "L")] >= shift[(w, sunday, "L")])
```

**Regra de coordenação**:
- **Implicação direccional**: domingo L → sábado L
- **Lógica**: evita padrão "sábado trabalho + domingo folga"
- **Benefício**: promove fins de semana completos de descanso

### 4.4 `salsa_2_free_days_week()` - Mínimo de Folgas Semanais
```python
def salsa_2_free_days_week(model, shift, workers, week_to_days_salsa, working_days, 
                          admissao_proporcional, data_admissao, data_demissao, 
                          fixed_days_off, fixed_LQs):
    for w in workers:
        worker_admissao = data_admissao.get(w, 0)
        worker_demissao = data_demissao.get(w, 0)
        
        for week, days in week_to_days_salsa.items():
            week_work_days = [d for d in days if d in working_days[w]]
            week_work_days.sort()
            
            if not week_work_days:
                continue
                
            week_work_days_set = set(week_work_days)
            fixed_days_week = week_work_days_set.intersection(set(fixed_days_off[w]))
            fixed_lqs_week = week_work_days_set.intersection(set(fixed_LQs[w]))
            
            # Cálculo proporcional para semanas de admissão/demissão
            is_admissao_week = (worker_admissao > 0 and worker_admissao in days)
            is_demissao_week = (worker_demissao > 0 and worker_demissao in days)
            
            if is_admissao_week or is_demissao_week:
                if admissao_proporcional:
                    if is_admissao_week:
                        days_after_admissao = len([d for d in week_work_days if d >= worker_admissao])
                        required_free_days = max(0, (days_after_admissao * 2) // 5)
                    elif is_demissao_week:
                        days_before_demissao = len([d for d in week_work_days if d <= worker_demissao])
                        required_free_days = max(0, (days_before_demissao * 2) // 5)
                else:
                    required_free_days = 2
            else:
                required_free_days = 2
            
            # Ajustar por folgas já fixas
            required_free_days = max(0, required_free_days - len(fixed_days_week) - len(fixed_lqs_week))
            
            if required_free_days > 0:
                free_day_vars = []
                for d in week_work_days:
                    if d not in fixed_days_week and d not in fixed_lqs_week:
                        for free_shift in ["L", "F", "LQ"]:
                            if (w, d, free_shift) in shift:
                                free_day_vars.append(shift[(w, d, free_shift)])
                
                if free_day_vars:
                    model.Add(sum(free_day_vars) >= required_free_days)
```

**Cálculo proporcional detalhado**:
- **Semana normal**: 2 folgas obrigatórias
- **Semana de admissão**: folgas proporcionais aos dias trabalhados após admissão
- **Semana de demissão**: folgas proporcionais aos dias trabalhados antes da demissão
- **Dedução de fixos**: folgas já pré-atribuídas reduzem a necessidade

### 4.5 `first_day_not_free()` - Trabalho Obrigatório no Primeiro Dia
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

### 4.6 `free_days_special_days()` - Folgas Mínimas em Domingos
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
def salsa_optimization(model, days_of_year, workers, working_shift, shift, pessObj, 
                      working_days, closed_holidays, min_workers, week_to_days, 
                      sundays, c2d, first_day, last_day, role_by_worker):
    
    # Dicionários para rastreamento
    pos_diff_dict = {}      # Desvios positivos por dia/turno
    neg_diff_dict = {}      # Desvios negativos por dia/turno
    no_workers_penalties = {}    # Penalizações por dias sem trabalhadores
    min_workers_penalties = {}   # Penalizações por staffing insuficiente
    inconsistent_shift_penalties = {}  # Penalizações por inconsistência semanal
    
    objective_terms = []    # Lista final de termos da função objectivo
```

## 2. Objetivos Principais (Operacionais)

### 2.1 Minimização de Desvios das Necessidades (`pess_obj`)
```python
# Constantes de peso
HEAVY_PENALTY = 300      # Dias sem trabalhadores
MIN_WORKER_PENALTY = 60  # Insuficiência de staffing
DEVIATION_WEIGHT = 1     # Peso base para desvios

# Para cada dia e turno
for d in days_of_year:
    for shift_type in ["M", "T"]:  # Apenas turnos de trabalho
        target = pessObj.get((d, shift_type), 0)  # Objetivo de pessoas
        
        # Calcular pessoas alocadas
        actual_workers = sum(shift.get((w, d, shift_type), 0) for w in workers)
        
        # Criar variáveis para desvios
        pos_diff = model.NewIntVar(0, len(workers), f"pos_diff_d{d}_s{shift_type}")
        neg_diff = model.NewIntVar(0, target, f"neg_diff_d{d}_s{shift_type}")
        
        # Definir desvios
        model.Add(pos_diff >= actual_workers - target)  # Excesso
        model.Add(neg_diff >= target - actual_workers)  # Déficit
        
        # Armazenar para referência
        pos_diff_dict[(d, shift_type)] = pos_diff
        neg_diff_dict[(d, shift_type)] = neg_diff
        
        # Penalização quadrática para grandes desvios
        if target > 0:
            quadratic_penalty_pos = model.NewIntVar(0, len(workers)**2, f"quad_pos_d{d}_s{shift_type}")
            quadratic_penalty_neg = model.NewIntVar(0, target**2, f"quad_neg_d{d}_s{shift_type}")
            
            # Aproximação linear da penalização quadrática
            model.AddMultiplicationEquality(quadratic_penalty_pos, [pos_diff, pos_diff])
            model.AddMultiplicationEquality(quadratic_penalty_neg, [neg_diff, neg_diff])
            
            objective_terms.extend([
                DEVIATION_WEIGHT * pos_diff,      # Penalização linear do excesso
                DEVIATION_WEIGHT * neg_diff,      # Penalização linear do déficit
                quadratic_penalty_pos,            # Penalização quadrática do excesso
                quadratic_penalty_neg             # Penalização quadrática do déficit
            ])
        else:
            objective_terms.extend([pos_diff, neg_diff])
```

**Lógica de penalização**:
- **Desvios lineares**: penalização proporcional ao desvio
- **Desvios quadráticos**: penalização crescente para grandes desvios
- **Simétrica**: excesso e déficit penalizados igualmente
- **Escalável**: penalização adapta-se ao tamanho da equipa

### 2.2 Penalizações por Violação de Staffing Crítico
```python
# Penalização por dias completamente sem trabalhadores
for d in days_of_year:
    total_workers_day = sum(shift.get((w, d, s), 0) 
                           for w in workers 
                           for s in working_shift)
    
    # Variável booleana: 1 se nenhum trabalhador no dia
    no_workers = model.NewBoolVar(f"no_workers_d{d}")
    model.Add(total_workers_day >= 1).OnlyEnforceIf(no_workers.Not())
    model.Add(total_workers_day == 0).OnlyEnforceIf(no_workers)
    
    no_workers_penalties[d] = no_workers
    objective_terms.append(HEAVY_PENALTY * no_workers)

# Penalização por staffing abaixo do mínimo
for d in days_of_year:
    min_required = min_workers.get(d, 1)
    total_workers_day = sum(shift.get((w, d, s), 0) 
                           for w in workers 
                           for s in working_shift)
    
    # Variável para déficit de trabalhadores
    min_deficit = model.NewIntVar(0, min_required, f"min_deficit_d{d}")
    model.Add(min_deficit >= min_required - total_workers_day)
    model.Add(min_deficit >= 0)
    
    min_workers_penalties[d] = min_deficit
    objective_terms.append(MIN_WORKER_PENALTY * min_deficit)
```

**Escalas de penalização**:
- **Dias sem trabalhadores**: 300 × número de dias (catastrófico)
- **Staffing insuficiente**: 60 × déficit de pessoas (grave)
- **Desvios normais**: 1 × desvio (normal)

## 3. Objetivos de Bem-estar

### 3.1 Incentivo a Dias de Folga Consecutivos
```python
consecutive_free_day_bonus = []
CONSECUTIVE_BONUS_WEIGHT = -1  # Negativo = bonificação

for w in workers:
    worker_days = sorted([d for d in days_of_year if d in working_days[w]])
    
    for i in range(len(worker_days) - 1):
        day1, day2 = worker_days[i], worker_days[i+1]
        
        if day2 == day1 + 1:  # Dias consecutivos
            # Variável: 1 se ambos os dias são folga
            both_free = model.NewBoolVar(f"consecutive_free_w{w}_d{day1}_{day2}")
            
            # Somar folgas nos dois dias
            free_day1 = sum(shift.get((w, day1, s), 0) for s in ["L", "F", "LQ"])
            free_day2 = sum(shift.get((w, day2, s), 0) for s in ["L", "F", "LQ"])
            
            # both_free = 1 sse ambos são folga
            model.Add(free_day1 + free_day2 >= 2).OnlyEnforceIf(both_free)
            model.Add(free_day1 + free_day2 <= 1).OnlyEnforceIf(both_free.Not())
            
            consecutive_free_day_bonus.append(both_free)

# Adicionar bonificação à função objectivo
objective_terms.extend([CONSECUTIVE_BONUS_WEIGHT * bonus for bonus in consecutive_free_day_bonus])
```

**Benefícios**:
- **Qualidade de vida**: períodos de descanso contínuos
- **Flexibilidade**: não força, apenas incentiva
- **Balanço**: peso moderado para não dominar outros objectivos

### 3.2 Balanceamento de Fins de Semana de Qualidade (C2D)
```python
C2D_BALANCE_PENALTY = 100
c2d_balance_penalties = []

for w in workers:
    target_c2d = c2d.get(w, 0)
    
    # Contar fins de semana de qualidade reais
    actual_c2d_vars = []
    for sunday in sundays:
        if sunday in working_days[w]:
            saturday = sunday - 1
            if saturday in working_days[w]:
                # Fim de semana de qualidade: sábado LQ + domingo L
                weekend_quality = model.NewBoolVar(f"c2d_w{w}_sun{sunday}")
                
                saturday_lq = shift.get((w, saturday, "LQ"), 0)
                sunday_l = shift.get((w, sunday, "L"), 0)
                
                model.Add(saturday_lq + sunday_l >= 2).OnlyEnforceIf(weekend_quality)
                model.Add(saturday_lq + sunday_l <= 1).OnlyEnforceIf(weekend_quality.Not())
                
                actual_c2d_vars.append(weekend_quality)
    
    if actual_c2d_vars:
        total_c2d = sum(actual_c2d_vars)
        
        # Variáveis para desvios do target
        c2d_over = model.NewIntVar(0, len(actual_c2d_vars), f"c2d_over_w{w}")
        c2d_under = model.NewIntVar(0, target_c2d, f"c2d_under_w{w}")
        
        model.Add(c2d_over >= total_c2d - target_c2d)
        model.Add(c2d_under >= target_c2d - total_c2d)
        
        c2d_balance_penalties.extend([
            C2D_BALANCE_PENALTY * c2d_over,
            C2D_BALANCE_PENALTY * c2d_under
        ])

objective_terms.extend(c2d_balance_penalties)
```

## 4. Objetivos de Consistência

### 4.1 Penalização de Inconsistência de Turnos Semanais
```python
INCONSISTENT_SHIFT_PENALTY = 3

for w in workers:
    for week in week_to_days.keys():
        week_days = [d for d in week_to_days[week] if d in working_days[w]]
        
        if len(week_days) >= 2:  # Apenas se trabalha múltiplos dias
            # Contar dias com turno manhã e tarde
            morning_days = sum(shift.get((w, d, "M"), 0) for d in week_days)
            afternoon_days = sum(shift.get((w, d, "T"), 0) for d in week_days)
            
            # Variável: 1 se tem tanto manhã quanto tarde na semana
            mixed_shifts = model.NewBoolVar(f"mixed_shifts_w{w}_week{week}")
            
            # mixed_shifts = 1 sse (morning_days >= 1 E afternoon_days >= 1)
            morning_any = model.NewBoolVar(f"morning_any_w{w}_week{week}")
            afternoon_any = model.NewBoolVar(f"afternoon_any_w{w}_week{week}")
            
            model.Add(morning_days >= 1).OnlyEnforceIf(morning_any)
            model.Add(morning_days == 0).OnlyEnforceIf(morning_any.Not())
            model.Add(afternoon_days >= 1).OnlyEnforceIf(afternoon_any)
            model.Add(afternoon_days == 0).OnlyEnforceIf(afternoon_any.Not())
            
            model.Add(morning_any + afternoon_any >= 2).OnlyEnforceIf(mixed_shifts)
            model.Add(morning_any + afternoon_any <= 1).OnlyEnforceIf(mixed_shifts.Not())
            
            inconsistent_shift_penalties[w, week] = mixed_shifts
            objective_terms.append(INCONSISTENT_SHIFT_PENALTY * mixed_shifts)
```

**Benefício operacional**:
- **Simplicidade de gestão**: equipas consistentes por semana
- **Redução de erros**: menos confusão nos horários
- **Flexibilidade entre semanas**: pode mudar semana a semana

### 4.2 Balanceamento de Folgas Dominicais Entre Colaboradores
```python
SUNDAY_BALANCE_PENALTY = 50
sunday_balance_penalties = []

# Criar variáveis para folgas dominicais por colaborador
sunday_free_vars = {}
workers_with_sundays = []

for w in workers:
    worker_sundays = [d for d in sundays if d in working_days[w]]
    if not worker_sundays:
        continue
    
    workers_with_sundays.append(w)
    
    # Variáveis booleanas para cada domingo
    worker_sunday_vars = []
    for sunday in worker_sundays:
        sunday_free = model.NewBoolVar(f"sunday_free_w{w}_d{sunday}")
        
        # Ligar a folgas reais (L ou F)
        free_shifts = shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0)
        model.Add(free_shifts >= 1).OnlyEnforceIf(sunday_free)
        model.Add(free_shifts == 0).OnlyEnforceIf(sunday_free.Not())
        
        worker_sunday_vars.append(sunday_free)
    
    # Total de domingos livres para este colaborador
    total_sunday_free = model.NewIntVar(0, len(worker_sundays), f"total_sunday_free_w{w}")
    model.Add(total_sunday_free == sum(worker_sunday_vars))
    sunday_free_vars[w] = total_sunday_free

# Balanceamento pairwise entre colaboradores
if len(workers_with_sundays) > 1:
    for i in range(len(workers_with_sundays)):
        for j in range(i + 1, len(workers_with_sundays)):
            w1, w2 = workers_with_sundays[i], workers_with_sundays[j]
            
            # Diferença absoluta entre folgas dominicais
            diff_pos = model.NewIntVar(0, max(len(sundays), len(sundays)), f"sunday_diff_pos_w{w1}_w{w2}")
            diff_neg = model.NewIntVar(0, max(len(sundays), len(sundays)), f"sunday_diff_neg_w{w1}_w{w2}")
            
            model.Add(diff_pos >= sunday_free_vars[w1] - sunday_free_vars[w2])
            model.Add(diff_neg >= sunday_free_vars[w2] - sunday_free_vars[w1])
            
            sunday_balance_penalties.extend([
                SUNDAY_BALANCE_PENALTY * diff_pos,
                SUNDAY_BALANCE_PENALTY * diff_neg
            ])

objective_terms.extend(sunday_balance_penalties)
```

### 4.3 Balanceamento de LQ Entre Colaboradores
```python
LQ_BALANCE_PENALTY = 50
lq_balance_penalties = []

# Identificar sábados (dias antes de domingos)
saturdays = [s - 1 for s in sundays if (s - 1) in days_of_year]

lq_worker_vars = {}
workers_with_lq = []

for w in workers:
    worker_saturdays = [d for d in saturdays if d in working_days[w]]
    if not worker_saturdays:
        continue
    
    workers_with_lq.append(w)
    
    # Contar LQs por colaborador
    worker_lq_vars = []
    for saturday in worker_saturdays:
        if (w, saturday, "LQ") in shift:
            worker_lq_vars.append(shift[(w, saturday, "LQ")])
    
    if worker_lq_vars:
        total_lq = model.NewIntVar(0, len(worker_lq_vars), f"total_lq_w{w}")
        model.Add(total_lq == sum(worker_lq_vars))
        lq_worker_vars[w] = total_lq

# Balanceamento pairwise de LQs
if len(workers_with_lq) > 1:
    for i in range(len(workers_with_lq)):
        for j in range(i + 1, len(workers_with_lq)):
            w1, w2 = workers_with_lq[i], workers_with_lq[j]
            
            if w1 in lq_worker_vars and w2 in lq_worker_vars:
                max_lq = max(c2d.get(w1, 0), c2d.get(w2, 0))
                
                lq_diff_pos = model.NewIntVar(0, max_lq, f"lq_diff_pos_w{w1}_w{w2}")
                lq_diff_neg = model.NewIntVar(0, max_lq, f"lq_diff_neg_w{w1}_w{w2}")
                
                model.Add(lq_diff_pos >= lq_worker_vars[w1] - lq_worker_vars[w2])
                model.Add(lq_diff_neg >= lq_worker_vars[w2] - lq_worker_vars[w1])
                
                lq_balance_penalties.extend([
                    LQ_BALANCE_PENALTY * lq_diff_pos,
                    LQ_BALANCE_PENALTY * lq_diff_neg
                ])

objective_terms.extend(lq_balance_penalties)
```

## 5. Construção da Função Objectivo Final

### 5.1 Agregação e Optimização
```python
# Criar função objectivo final
total_objective = sum(objective_terms)
model.Minimize(total_objective)

# Retornar variáveis auxiliares para debugging
return {
    'pos_diff_dict': pos_diff_dict,
    'neg_diff_dict': neg_diff_dict,
    'no_workers_penalties': no_workers_penalties,
    'min_workers_penalties': min_workers_penalties,
    'inconsistent_shift_penalties': inconsistent_shift_penalties
}
```

### 5.2 Hierarquia de Prioridades (por peso)
1. **Dias sem trabalhadores**: 300 (crítico)
2. **Balanceamento C2D**: 100 (importante)
3. **Staffing insuficiente**: 60 (importante)
4. **Balanceamento entre colaboradores**: 50 (moderado)
5. **Inconsistência semanal**: 3 (baixo)
6. **Desvios de necessidades**: 1 (base)
7. **Folgas consecutivas**: -1 (bonificação)

### 5.3 Análise de Conflitos e Trade-offs
- **Operacional vs. Bem-estar**: staffing mínimo vs. folgas consecutivas
- **Individual vs. Coletivo**: necessidades específicas vs. equidade entre colaboradores
- **Curto vs. Longo prazo**: necessidades diárias vs. padrões semanais
- **Flexibilidade vs. Consistência**: adaptação vs. previsibilidade

O sistema de pesos permite ajustar estes trade-offs conforme as prioridades organizacionais, criando soluções que equilibram eficácia operacional com satisfação dos colaboradores.

---

# Solver

O processo de resolução é executado pela função `solve()` que utiliza o OR-Tools CP-SAT solver:

## Configuração do Solver

### Parâmetros Principais
- `num_search_workers`: 8 (busca paralela)
- `max_time_in_seconds`: 600 (timeout)
- `log_search_progress`: controlo de logging
- `use_phase_saving`: otimização de busca
- `random_seed`: 42 (quando em modo teste)

### Otimizações Avançadas
- `cp_model_presolve`: True (simplificação automática)
- `cp_model_probing_level`: 3 (nível de dedução)
- `symmetry_level`: 4 (deteção de simetrias)
- `linearization_level`: 2 (relaxações lineares)

## Processamento da Solução

### Mapeamento de Turnos
```python
shift_mapping = {
    'M': 'M',    # Turno manhã
    'T': 'T',    # Turno tarde
    'F': 'F',    # Feriado encerrado
    'V': 'V',    # Dia vazio
    'A': 'A',    # Ausência
    'L': 'L',    # Folga normal
    'LQ': 'LQ',  # Folga qualidade
    '-': '-'     # Não atribuído
}
```

### Construção do Schedule
Para cada colaborador e dia:
1. Verificar qual turno foi atribuído (shift[(w,d,s)] == 1)
2. Aplicar mapeamento de turnos
3. Contar estatísticas (L, LQ, LD por colaborador)
4. Marcar dias não atribuídos como '-'

### Validação de Status
- **OPTIMAL**: solução ótima encontrada
- **FEASIBLE**: solução viável encontrada
- **INFEASIBLE**: problema sem solução
- **MODEL_INVALID**: modelo mal formado
- **UNKNOWN**: timeout ou limite de recursos

### Output
DataFrame com estrutura:
- Colunas: ['Worker'] + ['Day_1', 'Day_2', ..., 'Day_N']
- Linhas: um colaborador por linha
- Células: tipo de turno atribuído

---

# Classe `salsaAlgorithm`

A classe `SalsaAlgorithm` herda de `BaseAlgorithm` e implementa o padrão de três fases:

## Estrutura da Classe

### Inicialização
```python
def __init__(self, parameters=None, algo_name='salsa_algorithm', 
             project_name=PROJECT_NAME, process_id=0, 
             start_date='', end_date='')
```

**Parâmetros Default**:
- `max_continuous_working_days`: 6
- `shifts`: ["M", "T", "L", "LQ", "F", "A", "V", "-"]
- `check_shifts`: ['M', 'T', 'L', 'LQ']
- `working_shifts`: ["M", "T"]
- `settings`: configurações específicas (F_special_day, etc.)

## Métodos Principais

### 1. `adapt_data()`
**Função**: Processar dados de entrada
**Input**: 
- `data`: dicionário com medium_dataframes
- `algorithm_treatment_params`: parâmetros de tratamento

**Processo**:
1. Validação de estrutura de dados
2. Extração dos 3 dataframes obrigatórios
3. Chamada a `read_data_salsa()`
4. Retorno de dicionário com dados processados

### 2. `execute_algorithm()`
**Função**: Executar algoritmo de otimização
**Input**: `adapted_data` (output de adapt_data)

**Processo**:
1. Extração de todas as variáveis processadas
2. Criação do modelo CP (`cp_model.CpModel()`)
3. Criação de variáveis de decisão (`decision_variables()`)
4. Aplicação de todas as restrições
5. Definição da função objetivo (`salsa_optimization()`)
6. Chamada ao solver (`solve()`)
7. Retorno do DataFrame com schedule final

### 3. `format_results()`
**Função**: Formatar resultados para output final
**Input**: DataFrame do schedule

**Output**: Dicionário com:
- `schedule`: DataFrame original
- `metadata`: estatísticas e parâmetros utilizados
- `formatted_schedule`: formato melted (long format)
- `summary`: resumo de execução

## Fluxo de Execução Completo

```python
# 1. Inicialização
algorithm = SalsaAlgorithm(parameters=custom_params)

# 2. Processamento de dados
adapted_data = algorithm.adapt_data(input_data, treatment_params)

# 3. Execução do algoritmo
schedule_df = algorithm.execute_algorithm(adapted_data)

# 4. Formatação de resultados
final_results = algorithm.format_results(schedule_df)
```

## Características Especiais

### Gestão de Colaboradores Problemáticos
Funcionalidade para remover colaboradores temporariamente:
```python
DROP_WORKERS = [80001676, 80001677]  # IDs a remover
# Remove de todas as estruturas de dados
```

### Logging Detalhado
Registo completo de:
- Validações de entrada
- Estatísticas de processamento
- Progresso de otimização
- Métricas de qualidade da solução

### Tratamento de Erros
Gestão robusta de:
- Dados em falta ou inválidos
- Modelos infeasíveis
- Timeouts de solver
- Problemas de formatação

Esta arquitetura modular permite fácil manutenção, teste e extensão do algoritmo SALSA, mantendo separação clara entre processamento de dados, modelação matemática e apresentação de resultados.

# Solver e Execução

O algoritmo SALSA utiliza o solver CP-SAT (Constraint Programming - Satisfiability) do OR-Tools, um dos solvers de constraints mais avançados disponíveis. Esta secção detalha como o solver é configurado, como processa a resolução, e como extrai e formata os resultados.

## 1. Configuração Avançada do Solver

### 1.1 Parâmetros de Performance
```python
def configure_cp_sat_solver():
    solver = cp_model.CpSolver()
    
    # Limite de tempo (crítico para responsividade)
    solver.parameters.max_time_in_seconds = 600.0  # 10 minutos máximo
    
    # Estratégias de busca
    solver.parameters.search_branching = cp_model.PORTFOLIO_SEARCH
    solver.parameters.cp_model_presolve = cp_model.CpSolverParameters.ON
    solver.parameters.cp_model_probing_level = 2
    
    # Paralelismo (utiliza múltiplos cores)
    solver.parameters.num_search_workers = min(8, os.cpu_count())
    
    # Logging para debugging
    solver.parameters.log_search_progress = True
    solver.parameters.log_to_stdout = False
    
    # Optimizações específicas para scheduling
    solver.parameters.linearization_level = 2
    solver.parameters.use_implications = True
    solver.parameters.use_strong_propagation = True
    
    return solver
```

**Análise de parâmetros**:
- **Portfolio Search**: combina múltiplas estratégias de busca em paralelo
- **Presolve**: simplifica o modelo antes da busca principal
- **Probing Level**: nível de análise de implicações lógicas
- **Search Workers**: número de threads paralelos para busca
- **Linearization**: converte constraints não-lineares em lineares quando possível

### 1.2 Estratégias de Heurísticas
```python
# Configuração de heurísticas específicas para scheduling
solver.parameters.preferred_variable_order = cp_model.CpSolverParameters.IN_ORDER

# Estratégia de restart para escapar de mínimos locais
solver.parameters.restart_algorithms = [
    cp_model.CpSolverParameters.LUBY_RESTART,
    cp_model.CpSolverParameters.DL_MOVING_AVERAGE_RESTART
]

# Configuração de cortes para melhorar bounds
solver.parameters.cut_level = 1
solver.parameters.generate_all_cuts = True
```

### 1.3 Memory Management
```python
# Gestão de memória para problemas grandes
solver.parameters.max_memory_in_mb = 4096  # 4GB máximo
solver.parameters.use_buffered_logging = True

# Configuração de garbage collection
solver.parameters.share_objective_bounds = True
solver.parameters.share_level_zero_bounds = True
```

## 2. Processo de Resolução Detalhado

### 2.1 Fase de Presolve
```python
def analyze_model_before_solve(model):
    """Análise do modelo antes da resolução"""
    
    # Estatísticas do modelo
    num_variables = len(model.Proto().variables)
    num_constraints = len(model.Proto().constraints)
    num_bool_vars = sum(1 for var in model.Proto().variables 
                       if var.domain == [0, 1])
    
    print(f"Modelo criado:")
    print(f"  - Variáveis totais: {num_variables}")
    print(f"  - Variáveis booleanas: {num_bool_vars}")
    print(f"  - Constraints: {num_constraints}")
    
    # Estimativa de complexidade
    complexity_score = (num_variables * num_constraints) / 1000000
    if complexity_score > 10:
        print(f"  - Complexidade alta detectada: {complexity_score:.2f}")
        return suggest_model_optimizations()
    
    return True

def suggest_model_optimizations():
    """Sugestões para otimizar modelos complexos"""
    return {
        'reduce_time_horizon': 'Considerar menor período de planning',
        'aggregate_workers': 'Agrupar colaboradores similares',
        'simplify_constraints': 'Relaxar constraints menos críticas',
        'use_decomposition': 'Dividir em sub-problemas'
    }
```

**Optimizações automáticas do presolve**:
1. **Variable Elimination**: remove variáveis redundantes
2. **Constraint Propagation**: deduz valores obrigatórios
3. **Bound Tightening**: melhora limites das variáveis
4. **Symmetry Breaking**: adiciona constraints para quebrar simetrias
5. **Linear Relaxation**: analisa versão contínua para bounds

### 2.2 Algoritmo Principal de Busca
```python
def execute_search_with_monitoring(solver, model):
    """Execução com monitoramento detalhado"""
    
    # Callback para monitoramento de progresso
    class SolutionCallback(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            cp_model.CpSolverSolutionCallback.__init__(self)
            self.solution_count = 0
            self.best_objective = float('inf')
            self.start_time = time.time()
            self.improvement_log = []
        
        def on_solution_callback(self):
            current_time = time.time() - self.start_time
            current_objective = self.ObjectiveValue()
            
            if current_objective < self.best_objective:
                self.best_objective = current_objective
                improvement = {
                    'time': current_time,
                    'objective': current_objective,
                    'solution_number': self.solution_count
                }
                self.improvement_log.append(improvement)
                
                print(f"Solução {self.solution_count}: {current_objective} "
                      f"(tempo: {current_time:.1f}s)")
            
            self.solution_count += 1
    
    # Configurar callback
    solution_callback = SolutionCallback()
    
    # Resolver com monitoramento
    status = solver.SolveWithSolutionCallback(model, solution_callback)
    
    return status, solution_callback

def analyze_search_statistics(solver, callback):
    """Análise detalhada das estatísticas de busca"""
    
    stats = {
        'status': solver.StatusName(),
        'objective_value': solver.ObjectiveValue() if solver.ObjectiveValue() else None,
        'best_objective_bound': solver.BestObjectiveBound(),
        'num_conflicts': solver.NumConflicts(),
        'num_branches': solver.NumBranches(),
        'wall_time': solver.WallTime(),
        'user_time': solver.UserTime(),
        'deterministic_time': solver.DeterministicTime(),
        'num_booleans': solver.NumBooleans(),
        'num_integers': solver.NumIntegers()
    }
    
    # Análise de eficiência
    if stats['wall_time'] > 0:
        stats['branches_per_second'] = stats['num_branches'] / stats['wall_time']
        stats['conflicts_per_second'] = stats['num_conflicts'] / stats['wall_time']
    
    # Análise de qualidade da solução
    if stats['objective_value'] and stats['best_objective_bound']:
        gap = abs(stats['objective_value'] - stats['best_objective_bound'])
        stats['optimality_gap'] = gap / max(abs(stats['objective_value']), 1)
        stats['optimality_gap_percent'] = stats['optimality_gap'] * 100
    
    return stats
```

### 2.3 Estratégias de Branch-and-Bound
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

## 3. Extração e Processamento dos Resultados

### 3.1 Interpretação do Status do Solver
```python
def interpret_solver_status(status, solver):
    """Interpretação detalhada do status de resolução"""
    
    status_analysis = {
        cp_model.OPTIMAL: {
            'description': 'Solução ótima encontrada',
            'confidence': 'Máxima',
            'action': 'Usar solução diretamente',
            'quality_score': 1.0
        },
        cp_model.FEASIBLE: {
            'description': 'Solução viável encontrada (pode não ser ótima)',
            'confidence': 'Alta',
            'action': 'Verificar gap de otimalidade',
            'quality_score': calculate_feasible_quality(solver)
        },
        cp_model.INFEASIBLE: {
            'description': 'Nenhuma solução viável existe',
            'confidence': 'Máxima',
            'action': 'Relaxar constraints ou ajustar dados',
            'quality_score': 0.0
        },
        cp_model.UNKNOWN: {
            'description': 'Tempo limite atingido sem solução',
            'confidence': 'Baixa',
            'action': 'Aumentar tempo limite ou simplificar modelo',
            'quality_score': 0.0
        }
    }
    
    result = status_analysis.get(status, {
        'description': 'Status desconhecido',
        'confidence': 'Nenhuma',
        'action': 'Verificar configuração do solver',
        'quality_score': 0.0
    })
    
    # Adicionar informações específicas
    if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
        result['objective_value'] = solver.ObjectiveValue()
        result['solve_time'] = solver.WallTime()
        result['optimality_gap'] = calculate_optimality_gap(solver)
    
    return result

def calculate_feasible_quality(solver):
    """Calcula qualidade de solução viável baseada no gap"""
    objective = solver.ObjectiveValue()
    bound = solver.BestObjectiveBound()
    
    if objective == 0:
        return 1.0 if bound == 0 else 0.5
    
    gap = abs(objective - bound) / abs(objective)
    return max(0.0, 1.0 - gap)
```

### 3.2 Extração Detalhada da Solução
```python
def extract_comprehensive_solution(solver, model_variables, workers, days_of_year, 
                                  working_shift, auxiliary_vars):
    """Extração completa da solução com análise"""
    
    solution_data = {
        'assignments': {},      # (worker, day, shift) -> value
        'worker_statistics': {},  # Estatísticas por colaborador
        'daily_coverage': {},     # Cobertura por dia
        'constraint_violations': {},  # Violações detectadas
        'objective_breakdown': {}     # Decomposição da função objectivo
    }
    
    # 1. Extrair atribuições principais
    for w in workers:
        solution_data['worker_statistics'][w] = {
            'total_work_days': 0,
            'total_free_days': 0,
            'morning_shifts': 0,
            'afternoon_shifts': 0,
            'quality_weekends': 0,
            'sunday_free_days': 0
        }
        
        for d in days_of_year:
            for s in working_shift:
                if (w, d, s) in model_variables['shift']:
                    value = solver.Value(model_variables['shift'][(w, d, s)])
                    solution_data['assignments'][(w, d, s)] = value
                    
                    # Atualizar estatísticas
                    if value == 1:
                        if s in ['M', 'T']:
                            solution_data['worker_statistics'][w]['total_work_days'] += 1
                            if s == 'M':
                                solution_data['worker_statistics'][w]['morning_shifts'] += 1
                            elif s == 'T':
                                solution_data['worker_statistics'][w]['afternoon_shifts'] += 1
                        else:
                            solution_data['worker_statistics'][w]['total_free_days'] += 1
    
    # 2. Calcular cobertura diária
    for d in days_of_year:
        solution_data['daily_coverage'][d] = {
            'morning_workers': 0,
            'afternoon_workers': 0,
            'total_workers': 0,
            'coverage_quality': 'adequate'
        }
        
        for w in workers:
            if solution_data['assignments'].get((w, d, 'M'), 0):
                solution_data['daily_coverage'][d]['morning_workers'] += 1
            if solution_data['assignments'].get((w, d, 'T'), 0):
                solution_data['daily_coverage'][d]['afternoon_workers'] += 1
        
        total = (solution_data['daily_coverage'][d]['morning_workers'] + 
                solution_data['daily_coverage'][d]['afternoon_workers'])
        solution_data['daily_coverage'][d]['total_workers'] = total
        
        # Avaliar qualidade da cobertura
        if total == 0:
            solution_data['daily_coverage'][d]['coverage_quality'] = 'critical'
        elif total < 2:
            solution_data['daily_coverage'][d]['coverage_quality'] = 'minimal'
        elif total >= 4:
            solution_data['daily_coverage'][d]['coverage_quality'] = 'excellent'
    
    # 3. Verificar violações de constraints
    solution_data['constraint_violations'] = validate_solution_constraints(
        solution_data['assignments'], workers, days_of_year
    )
    
    # 4. Decompor função objectivo
    if 'optimization_vars' in auxiliary_vars:
        solution_data['objective_breakdown'] = decompose_objective_function(
            solver, auxiliary_vars['optimization_vars']
        )
    
    return solution_data

def validate_solution_constraints(assignments, workers, days_of_year):
    """Validação das constraints na solução final"""
    
    violations = {
        'unique_assignment_violations': [],
        'consecutive_work_violations': [],
        'weekly_inconsistency_violations': [],
        'minimum_rest_violations': []
    }
    
    # Verificar atribuição única por dia
    for w in workers:
        for d in days_of_year:
            day_assignments = sum(assignments.get((w, d, s), 0) 
                                 for s in ['M', 'T', 'L', 'F', 'LQ'])
            if day_assignments != 1:
                violations['unique_assignment_violations'].append(
                    f"Worker {w}, Day {d}: {day_assignments} assignments"
                )
    
    # Verificar limite de dias consecutivos
    for w in workers:
        consecutive_work = 0
        for d in sorted(days_of_year):
            is_work_day = (assignments.get((w, d, 'M'), 0) + 
                          assignments.get((w, d, 'T'), 0))
            
            if is_work_day:
                consecutive_work += 1
                if consecutive_work > 6:  # Máximo 6 dias consecutivos
                    violations['consecutive_work_violations'].append(
                        f"Worker {w}, Day {d}: {consecutive_work} consecutive days"
                    )
            else:
                consecutive_work = 0
    
    return violations

def decompose_objective_function(solver, optimization_vars):
    """Decomposição da função objectivo por componente"""
    
    breakdown = {
        'deviation_penalties': 0,
        'no_workers_penalties': 0,
        'min_workers_penalties': 0,
        'c2d_balance_penalties': 0,
        'inconsistent_shift_penalties': 0,
        'consecutive_free_bonuses': 0,
        'sunday_balance_penalties': 0,
        'lq_balance_penalties': 0
    }
    
    # Calcular cada componente
    for var_name, var_list in optimization_vars.items():
        if isinstance(var_list, list):
            component_value = sum(solver.Value(var) for var in var_list)
        else:
            component_value = solver.Value(var_list)
        
        breakdown[var_name] = component_value
    
    breakdown['total_objective'] = solver.ObjectiveValue()
    
    return breakdown
```

### 3.3 Formatação dos Resultados Finais
```python
def format_comprehensive_results(solution_data, model_info):
    """Formatação final dos resultados para output"""
    
    formatted_results = {
        'metadata': {
            'algorithm': 'SALSA',
            'solver': 'CP-SAT',
            'solve_time': model_info.get('solve_time', 0),
            'status': model_info.get('status', 'UNKNOWN'),
            'objective_value': model_info.get('objective_value', None),
            'solution_quality': model_info.get('quality_score', 0)
        },
        'schedule': convert_assignments_to_schedule_format(solution_data['assignments']),
        'analytics': {
            'worker_summary': solution_data['worker_statistics'],
            'daily_coverage_summary': solution_data['daily_coverage'],
            'constraint_compliance': solution_data['constraint_violations'],
            'objective_breakdown': solution_data['objective_breakdown']
        },
        'recommendations': generate_recommendations(solution_data),
        'warnings': generate_warnings(solution_data)
    }
    
    return formatted_results

def convert_assignments_to_schedule_format(assignments):
    """Converte atribuições para formato de horário legível"""
    
    schedule = {}
    
    for (worker, day, shift), value in assignments.items():
        if value == 1:
            if worker not in schedule:
                schedule[worker] = {}
            
            schedule[worker][day] = {
                'shift_type': shift,
                'readable_shift': {
                    'M': 'Manhã (08:00-16:00)',
                    'T': 'Tarde (16:00-00:00)',
                    'L': 'Folga',
                    'F': 'Férias',
                    'LQ': 'Folga Qualidade (Sábado)'
                }.get(shift, shift)
            }
    
    return schedule

def generate_recommendations(solution_data):
    """Gera recomendações baseadas na análise da solução"""
    
    recommendations = []
    
    # Análise de cobertura
    critical_days = [d for d, coverage in solution_data['daily_coverage'].items()
                    if coverage['coverage_quality'] == 'critical']
    
    if critical_days:
        recommendations.append({
            'type': 'coverage_critical',
            'message': f'Días críticos detectados: {critical_days}',
            'action': 'Considerar relaxar constraints ou adicionar trabalhadores'
        })
    
    # Análise de balanceamento
    worker_stats = solution_data['worker_statistics']
    work_day_counts = [stats['total_work_days'] for stats in worker_stats.values()]
    
    if len(work_day_counts) > 1:
        work_variance = statistics.variance(work_day_counts)
        if work_variance > 10:  # Alta variância
            recommendations.append({
                'type': 'workload_imbalance',
                'message': 'Desequilíbrio de carga de trabalho detectado',
                'action': 'Aumentar peso de penalizações de balanceamento'
            })
    
    return recommendations

def generate_warnings(solution_data):
    """Gera avisos sobre potenciais problemas"""
    
    warnings = []
    
    # Verificar violações
    violations = solution_data['constraint_violations']
    for violation_type, violation_list in violations.items():
        if violation_list:
            warnings.append({
                'type': violation_type,
                'count': len(violation_list),
                'details': violation_list[:5]  # Primeiros 5 para não sobrecarregar
            })
    
    return warnings
```

## 4. Estratégias de Fallback e Recovery

### 4.1 Tratamento de Problemas Infeasible
```python
def handle_infeasible_solution(model, workers, days_of_year):
    """Tratamento sistemático de problemas inviáveis"""
    
    # 1. Análise de constraints conflituosas
    conflicting_constraints = identify_conflicting_constraints(model)
    
    # 2. Relaxação hierárquica de constraints
    relaxation_strategies = [
        ('soft_min_workers', 'Relaxar requisitos mínimos de trabalhadores'),
        ('soft_c2d_targets', 'Relaxar targets de fins de semana de qualidade'),
        ('allow_mixed_shifts', 'Permitir turnos mistos na mesma semana'),
        ('extend_consecutive_work', 'Permitir mais dias consecutivos de trabalho'),
        ('reduce_balance_requirements', 'Reduzir exigências de balanceamento')
    ]
    
    for strategy_name, description in relaxation_strategies:
        print(f"Tentando relaxação: {description}")
        
        relaxed_model = create_relaxed_model(model, strategy_name)
        status = solve_relaxed_model(relaxed_model)
        
        if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
            print(f"Solução encontrada com relaxação: {strategy_name}")
            return extract_solution_with_warnings(relaxed_model, strategy_name)
    
    # 3. Fallback para solução mínima viável
    return create_minimal_viable_solution(workers, days_of_year)

def identify_conflicting_constraints(model):
    """Identifica constraints que podem estar em conflito"""
    
    # Usar solver para encontrar core minimal de constraints conflituosas
    assumptions = []
    for i, constraint in enumerate(model.Proto().constraints):
        assumption_var = model.NewBoolVar(f'assumption_{i}')
        assumptions.append(assumption_var)
        # Adicionar implicação: assumption_var => constraint
    
    return assumptions

def create_minimal_viable_solution(workers, days_of_year):
    """Cria solução mínima que garante cobertura básica"""
    
    minimal_solution = {}
    
    # Algoritmo simples round-robin para garantir cobertura
    for i, d in enumerate(sorted(days_of_year)):
        # Atribuir um trabalhador para manhã e outro para tarde
        morning_worker = workers[i % len(workers)]
        afternoon_worker = workers[(i + 1) % len(workers)]
        
        minimal_solution[(morning_worker, d, 'M')] = 1
        minimal_solution[(afternoon_worker, d, 'T')] = 1
        
        # Outros trabalhadores ficam em folga
        for w in workers:
            if w not in [morning_worker, afternoon_worker]:
                minimal_solution[(w, d, 'L')] = 1
    
    return {
        'status': 'MINIMAL_VIABLE',
        'assignments': minimal_solution,
        'warnings': ['Solução mínima criada devido a inviabilidade do modelo original']
    }
```

### 4.2 Otimização de Performance
```python
def optimize_solver_performance(model_size, available_time):
    """Otimização automática dos parâmetros do solver"""
    
    if model_size['variables'] > 10000:
        # Modelo grande - foco em encontrar soluções rapidamente
        return {
            'search_branching': cp_model.AUTOMATIC_SEARCH,
            'max_time_in_seconds': min(available_time, 1800),  # Max 30 min
            'num_search_workers': min(16, os.cpu_count()),
            'linearization_level': 1,
            'cp_model_presolve': cp_model.CpSolverParameters.ON
        }
    
    elif model_size['variables'] > 5000:
        # Modelo médio - equilíbrio entre velocidade e qualidade
        return {
            'search_branching': cp_model.PORTFOLIO_SEARCH,
            'max_time_in_seconds': min(available_time, 900),   # Max 15 min
            'num_search_workers': min(8, os.cpu_count()),
            'linearization_level': 2,
            'cp_model_presolve': cp_model.CpSolverParameters.ON
        }
    
    else:
        # Modelo pequeno - foco na otimalidade
        return {
            'search_branching': cp_model.PORTFOLIO_SEARCH,
            'max_time_in_seconds': min(available_time, 300),   # Max 5 min
            'num_search_workers': min(4, os.cpu_count()),
            'linearization_level': 2,
            'cp_model_presolve': cp_model.CpSolverParameters.ON,
            'use_implications': True,
            'use_strong_propagation': True
        }
```

## 5. Monitoramento e Debugging

### 5.1 Logging Detalhado
```python
class SalsaSolverLogger:
    def __init__(self, log_level='INFO'):
        self.log_level = log_level
        self.search_log = []
        self.performance_metrics = {}
        
    def log_model_creation(self, model_stats):
        self.log(f"Modelo SALSA criado: {model_stats['variables']} vars, "
                f"{model_stats['constraints']} constraints")
    
    def log_solver_start(self, solver_config):
        self.log(f"Iniciando resolver com configuração: {solver_config}")
    
    def log_solution_progress(self, iteration, objective, time_elapsed):
        self.log(f"Iteração {iteration}: objetivo={objective:.2f}, "
                f"tempo={time_elapsed:.1f}s")
        self.search_log.append({
            'iteration': iteration,
            'objective': objective,
            'time': time_elapsed
        })
    
    def log_final_result(self, status, final_objective, total_time):
        self.log(f"Resolução finalizada: {status}, "
                f"objetivo={final_objective}, tempo_total={total_time:.1f}s")
        
        self.performance_metrics = {
            'final_status': status,
            'final_objective': final_objective,
            'total_solve_time': total_time,
            'iterations_logged': len(self.search_log),
            'average_improvement_time': self.calculate_average_improvement_time()
        }
    
    def calculate_average_improvement_time(self):
        if len(self.search_log) < 2:
            return 0
        
        improvements = []
        for i in range(1, len(self.search_log)):
            if self.search_log[i]['objective'] < self.search_log[i-1]['objective']:
                time_diff = self.search_log[i]['time'] - self.search_log[i-1]['time']
                improvements.append(time_diff)
        
        return sum(improvements) / len(improvements) if improvements else 0
```

### 5.2 Análise Post-Mortem
```python
def analyze_solver_performance(solver_stats, solution_quality):
    """Análise completa da performance do solver"""
    
    analysis = {
        'efficiency_metrics': {
            'solve_speed': solver_stats['num_branches'] / max(solver_stats['wall_time'], 0.001),
            'conflict_rate': solver_stats['num_conflicts'] / max(solver_stats['num_branches'], 1),
            'presolve_effectiveness': calculate_presolve_reduction(solver_stats),
            'memory_efficiency': solver_stats.get('peak_memory_mb', 0) / 1024  # GB
        },
        'solution_quality': {
            'optimality_confidence': solution_quality.get('quality_score', 0),
            'constraint_satisfaction': calculate_constraint_satisfaction(solution_quality),
            'objective_breakdown': solution_quality.get('objective_breakdown', {}),
            'balance_achieved': calculate_balance_metrics(solution_quality)
        },
        'recommendations': {
            'parameter_tuning': suggest_parameter_improvements(solver_stats),
            'model_modifications': suggest_model_improvements(solution_quality),
            'scaling_advice': suggest_scaling_approach(solver_stats)
        }
    }
    
    return analysis

def suggest_parameter_improvements(solver_stats):
    """Sugestões para melhorar parâmetros do solver"""
    
    suggestions = []
    
    if solver_stats['wall_time'] > 300:  # Mais de 5 minutos
        suggestions.append("Considerar aumentar num_search_workers para paralelismo")
        suggestions.append("Reduzir linearization_level para acelerar busca")
    
    if solver_stats['num_conflicts'] / solver_stats['num_branches'] > 0.8:
        suggestions.append("Alta taxa de conflitos - aumentar cp_model_probing_level")
        suggestions.append("Considerar different restart strategy")
    
    if solver_stats.get('optimality_gap_percent', 0) > 5:
        suggestions.append("Gap de otimalidade alto - aumentar max_time_in_seconds")
        suggestions.append("Usar search_branching = PORTFOLIO_SEARCH")
    
    return suggestions
```

O solver CP-SAT representa o estado da arte em resolução de problemas de constraint programming, utilizando técnicas avançadas de propagação, busca inteligente e otimização para encontrar soluções de alta qualidade para o complexo problema de scheduling do algoritmo SALSA.

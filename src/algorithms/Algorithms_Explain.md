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

Esta matriz representa as necessidades para um respetivo dia, num posto e para os diferentes tipos de turno (Manhã ou Tarde). Vai ser a principal

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

# Leitura e Tratamento de Dados

---

# Criação das Variáveis

---

# Restrições

---

# Otimização

---

# Solver

---

# Classe `salsaAlgorithm`

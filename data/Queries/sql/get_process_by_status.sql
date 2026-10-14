select * 
from wfm.esc_processo
where 1=1
and tipo = 'AJH'
--and tipo in ('MPD', 'ESC')
--and fk_secao in ('2936', '2937')
--and fk_secao in ('2739')
--and fk_colaborador is not null
and situacao = 'N'
--and codigo = '22609'
order by codigo desc
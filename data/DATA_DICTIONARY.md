# Dicionário de dados — ChuvaSegura

**Escopo:** tabelas persistidas no Unity Catalog `fiap_analytics`, nas camadas Bronze, Silver e Gold. As definições refletem os contratos e transformações versionados neste repositório; a execução no Databricks pode ter dados e schemas mais recentes.

Este documento complementa o [guia do pipeline](README.md), que descreve fontes, downloads, regras de qualidade, cargas e contexto operacional. O catálogo do Unity Catalog deve ser consultado para confirmar o schema efetivamente implantado.

## Convenções

- **Bronze:** preserva dados de origem, em geral como texto, com metadados de ingestão. Colunas específicas da fonte mantêm o nome original ou o nome curto atribuído no carregamento; consulte a seção da fonte no [guia do pipeline](README.md) e o notebook Bronze correspondente.
- **Silver:** aplica tipos, padronizações, deduplicação e convenções de tempo/geometria. A granularidade e as transformações por fonte estão descritas neste documento e no guia do pipeline.
- **Gold:** modelo analítico de fatos e dimensões. As descrições de cada coluna estão nos comentários do DDL SQL, que são a referência de coluna para as tabelas Gold.
- Campos de geometria são identificados por nome e sistema de referência espacial (EPSG) onde disponível. Valores temporais em UTC e no fuso de São Paulo são explicitados quando aplicável.
- `-1` identifica o membro “não informado” nas dimensões Gold que declaram esse registro.

## Inventário de tabelas

| Camada | Tabela (`fiap_analytics`) | Granularidade / conteúdo | Fonte |
|---|---|---|---|
| Bronze | `bronze.cemaden_stations` | Snapshot do cadastro de estações; dados da fonte e metadados de carga | CEMADEN |
| Bronze | `bronze.cemaden_rainfall_readings` | Leituras brutas de chuva a partir de 2020, particionadas por janela mensal | CEMADEN |
| Bronze | `bronze.sibh_stations` | Snapshots do cadastro de sensores do estado de São Paulo | SIBH / SP Águas |
| Bronze | `bronze.sibh_measurements` | Medições brutas de chuva e nível, em janelas mensais | SIBH / SP Águas |
| Bronze | `bronze.geosampa_history_flooding_events` | Ocorrências históricas; geometria de origem em WKB | GeoSampa |
| Bronze | `bronze.geosampa_subprefecture_area` | Polígonos e agrupamentos de subprefeituras | GeoSampa |
| Bronze | `bronze.sgb_susceptibility_areas` | Feições das camadas da carta de suscetibilidade; geometria WKB em EPSG:31983 | SGB / IPT |
| Bronze | `bronze.ibge_census_tracts` | Setores censitários de São Paulo; atributos brutos e geometria WKB em EPSG:4674 | IBGE, Censo 2022 |
| Bronze | `bronze.osm_roads` | Vias no recorte municipal; atributos de origem e geometria WKB em EPSG:4326 | OpenStreetMap |
| Bronze | `bronze.inmet_weather_readings` | Observações meteorológicas horárias brutas a partir de 2020, em janelas anuais | INMET |
| Silver | `silver.cemaden_stations` | Uma linha por estação, usando o snapshot mais recente em que ela aparece | CEMADEN |
| Silver | `silver.cemaden_rainfall_readings` | Leituras tipadas e deduplicadas por estação, sensor e instante; guarda instante UTC e data/hora local | CEMADEN |
| Silver | `silver.sibh_stations` | Uma linha por sensor de chuva ou nível na capital | SIBH / SP Águas |
| Silver | `silver.sibh_measurements` | Medições tipadas e deduplicadas por sensor e instante; timestamp UTC e calendário local | SIBH / SP Águas |
| Silver | `silver.geosampa_flooding_events` | Uma linha por ocorrência, com identificador, data, tipo, subprefeitura, coordenadas e WKB | GeoSampa |
| Silver | `silver.sgb_susceptibility_areas` | Uma linha por feição, com camada/classe padronizadas, geometria EPSG:4674, área, centroide e envelope | SGB / IPT |
| Silver | `silver.ibge_census_tracts` | Uma linha por setor censitário, com atributos tipados e geometrias/medidas derivadas | IBGE, Censo 2022 |
| Silver | `silver.osm_roads` | Uma linha por via, com classe e atributos tipados, geometria EPSG:4674 e comprimento em UTM 23S | OpenStreetMap |
| Silver | `silver.inmet_stations` | Uma linha por estação automática do INMET na capital | INMET |
| Silver | `silver.inmet_weather_readings` | Observações tipadas e deduplicadas por estação e hora; mantém hora UTC e hora local | INMET |
| Gold | `gold.dim_tempo` | Uma linha por dia; calendário compartilhado pelas fatos | Compartilhada |
| Gold | `gold.dim_cemaden_stations` | Uma linha por estação CEMADEN, incluindo membro desconhecido `-1` | CEMADEN |
| Gold | `gold.fact_cemaden_rainfall_hourly` | Uma linha por estação, sensor, dia e hora local; agrega leituras de chuva | CEMADEN |
| Gold | `gold.dim_sibh_sensors` | Uma linha por sensor SIBH, incluindo membro desconhecido `-1` | SIBH / SP Águas |
| Gold | `gold.fact_sibh_rainfall_hourly` | Uma linha por sensor, dia e hora local; agrega chuva | SIBH / SP Águas |
| Gold | `gold.fact_sibh_level_hourly` | Uma linha por sensor, dia e hora local; resume nível médio, mínimo, máximo e último | SIBH / SP Águas |
| Gold | `gold.dim_geosampa_subprefecture_area` | Uma linha por subprefeitura/agrupamento, incluindo membro desconhecido `-1` | GeoSampa |
| Gold | `gold.dim_geosampa_type_flooding_events` | Uma linha por tipo de ocorrência, incluindo membro desconhecido `-1` | GeoSampa |
| Gold | `gold.fact_geosampa_flooding_events` | Uma linha por ocorrência; chave natural composta por ano de origem e ID da ocorrência | GeoSampa |
| Gold | `gold.dim_sgb_susceptibility_class` | Uma linha por camada e classe de suscetibilidade, incluindo membro `-1` | SGB / IPT |
| Gold | `gold.dim_sgb_susceptibility_areas` | Uma linha por polígono, com classe, geometria e atributos espaciais, incluindo membro `-1` | SGB / IPT |
| Gold | `gold.dim_ibge_census_tracts` | Uma linha por setor censitário, com dimensão espacial e membro `-1` | IBGE, Censo 2022 |
| Gold | `gold.fact_ibge_census_tract_population` | Uma linha por setor na data de referência do Censo 2022 | IBGE, Censo 2022 |
| Gold | `gold.dim_osm_road_class` | Uma linha por classe de via e atributos de hierarquia, incluindo membro `-1` | OpenStreetMap |
| Gold | `gold.dim_osm_roads` | Uma linha por via, com atributos espaciais e membro `-1` | OpenStreetMap |
| Gold | `gold.dim_inmet_stations` | Uma linha por estação, incluindo membro desconhecido `-1` | INMET |
| Gold | `gold.fact_inmet_weather_hourly` | Uma linha por estação, dia e hora local; medidas meteorológicas agregadas | INMET |

### Tabelas operacionais

As tabelas abaixo são persistidas no schema Bronze e complementam as 37 tabelas de dados acima. Há uma tabela por fonte, todas com o mesmo contrato.

| Tabela | Granularidade | Colunas e definição |
|---|---|---|
| `bronze.cemaden_ingestion_log` | Uma linha por janela/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.sibh_ingestion_log` | Uma linha por janela/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.geosampa_ingestion_log` | Uma linha por arquivo/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.sgb_ingestion_log` | Uma linha por pacote/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.ibge_ingestion_log` | Uma linha por arquivo/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.osm_ingestion_log` | Uma linha por extrato/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.inmet_ingestion_log` | Uma linha por ano/etapa de execução | Ver [contrato do log de ingestão](#contrato-do-log-de-ingestão). |
| `bronze.cemaden_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |
| `bronze.sibh_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |
| `bronze.geosampa_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |
| `bronze.sgb_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |
| `bronze.ibge_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |
| `bronze.osm_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |
| `bronze.inmet_data_quality_log` | Uma linha por regra avaliada em uma execução | Ver [contrato do log de qualidade](#contrato-do-log-de-qualidade). |

## Contrato do log de ingestão

DDL e schema de referência: [`src/common/ingestion_log.py`](src/common/ingestion_log.py).

| Coluna | Tipo | Definição |
|---|---|---|
| `batch_id` | STRING | Identificador da execução do job. |
| `fonte` | STRING | Conjunto de dados/origem processado. |
| `etapa` | STRING | Etapa registrada: `landing` ou `bronze`. |
| `janela` | STRING | Período processado, como mês, data do snapshot ou ano do arquivo. |
| `status` | STRING | Resultado: `SUCESSO`, `PARCIAL`, `SEM_DADOS` ou `FALHA`. |
| `registros` | BIGINT | Quantidade de registros processados, quando disponível. |
| `paginas` | INT | Quantidade de páginas/chamadas à API, quando aplicável. |
| `duplicados` | BIGINT | Duplicidades detectadas pela chave natural da origem, quando aplicável. |
| `arquivo` | STRING | Caminho ou identificação do arquivo processado, quando aplicável. |
| `sha256` | STRING | Hash SHA-256 usado para identificar/verificar o conteúdo de origem. |
| `duracao_s` | DOUBLE | Duração da etapa em segundos. |
| `iniciado_em` | TIMESTAMP | Instante de início da etapa. |
| `finalizado_em` | TIMESTAMP | Instante de término da etapa. |
| `mensagem` | STRING | Detalhe de erro, aviso ou informação operacional; limitado a 1.000 caracteres. |

## Contrato do log de qualidade

DDL e schema de referência: [`src/common/data_quality.py`](src/common/data_quality.py). As regras e os limites por fonte estão na seção [Qualidade de dados](README.md#qualidade-de-dados).

| Coluna | Tipo | Definição |
|---|---|---|
| `batch_id` | STRING | Identificador da execução avaliada. |
| `fonte` | STRING | Origem avaliada. |
| `etapa` | STRING | Camada avaliada: Bronze, Silver ou Gold. |
| `tabela` | STRING | Tabela submetida à regra. |
| `regra` | STRING | Identificador/nome da regra de qualidade. |
| `descricao` | STRING | Explicação do que a regra verifica. |
| `severidade` | STRING | `ERRO` interrompe a tarefa quando falha; `AVISO` apenas registra. |
| `status` | STRING | Resultado da regra: `SUCESSO` ou `FALHA`. |
| `falhas` | BIGINT | Número de linhas que violaram a regra. |
| `total` | BIGINT | Número de linhas avaliadas. |
| `percentual` | DOUBLE | Proporção de falhas em relação ao total. |
| `limite` | DOUBLE | Proporção máxima aceita; zero significa nenhuma falha permitida. |
| `executado_em` | TIMESTAMP | Instante em que a regra foi executada. |
| `mensagem` | STRING | Mensagem adicional sobre a avaliação, se houver. |

## Dicionário de colunas Gold

Os DDLs Gold contêm `COMMENT` em todas as colunas e na tabela. Para preservar o alinhamento entre catálogo e código, esses comentários são a definição canônica dos campos. Cada nome abaixo abre o DDL da tabela correspondente:

| Tabela | DDL com comentários por coluna |
|---|---|
| `gold.dim_tempo` | [`00_dim_tempo.sql`](src/shared/transformations/gold/00_dim_tempo.sql) |
| `gold.dim_cemaden_stations` | [`04_dim_cemaden_stations.sql`](src/cemaden/transformations/gold/04_dim_cemaden_stations.sql) |
| `gold.fact_cemaden_rainfall_hourly` | [`05_fact_cemaden_rainfall_hourly.sql`](src/cemaden/transformations/gold/05_fact_cemaden_rainfall_hourly.sql) |
| `gold.dim_sibh_sensors` | [`06_dim_sibh_sensors.sql`](src/sibh/transformations/gold/06_dim_sibh_sensors.sql) |
| `gold.fact_sibh_rainfall_hourly` | [`07_fact_sibh_rainfall_hourly.sql`](src/sibh/transformations/gold/07_fact_sibh_rainfall_hourly.sql) |
| `gold.fact_sibh_level_hourly` | [`07_fact_sibh_level_hourly.sql`](src/sibh/transformations/gold/07_fact_sibh_level_hourly.sql) |
| `gold.dim_geosampa_subprefecture_area` | [`03_dim_geosampa_subprefecture_area.sql`](src/geosampa/transformations/gold/03_dim_geosampa_subprefecture_area.sql) |
| `gold.dim_geosampa_type_flooding_events` | [`03_dim_geosampa_type_flooding_events.sql`](src/geosampa/transformations/gold/03_dim_geosampa_type_flooding_events.sql) |
| `gold.fact_geosampa_flooding_events` | [`04_fact_geosampa_flooding_events.sql`](src/geosampa/transformations/gold/04_fact_geosampa_flooding_events.sql) |
| `gold.dim_sgb_susceptibility_class` | [`04_dim_sgb_susceptibility_class.sql`](src/sgb/transformations/gold/04_dim_sgb_susceptibility_class.sql) |
| `gold.dim_sgb_susceptibility_areas` | [`05_dim_sgb_susceptibility_areas.sql`](src/sgb/transformations/gold/05_dim_sgb_susceptibility_areas.sql) |
| `gold.dim_ibge_census_tracts` | [`04_dim_ibge_census_tracts.sql`](src/ibge/transformations/gold/04_dim_ibge_census_tracts.sql) |
| `gold.fact_ibge_census_tract_population` | [`05_fact_ibge_census_tract_population.sql`](src/ibge/transformations/gold/05_fact_ibge_census_tract_population.sql) |
| `gold.dim_osm_road_class` | [`04_dim_osm_road_class.sql`](src/osm/transformations/gold/04_dim_osm_road_class.sql) |
| `gold.dim_osm_roads` | [`05_dim_osm_roads.sql`](src/osm/transformations/gold/05_dim_osm_roads.sql) |
| `gold.dim_inmet_stations` | [`04_dim_inmet_stations.sql`](src/inmet/transformations/gold/04_dim_inmet_stations.sql) |
| `gold.fact_inmet_weather_hourly` | [`05_fact_inmet_weather_hourly.sql`](src/inmet/transformations/gold/05_fact_inmet_weather_hourly.sql) |

## Atributos de origem e transformações Silver

As tabelas Bronze mantêm as colunas recebidas das fontes ou nomes curtos definidos no parser. Os atributos de origem podem variar conforme a fonte e a versão do arquivo; a documentação autoritativa de seu significado de negócio é a respectiva fonte pública. Os campos técnicos adicionados pelo pipeline são:

| Campo técnico | Definição |
|---|---|
| `_source_file` | Caminho/nome do arquivo de origem associado ao registro. |
| `_source_layer` | Camada do arquivo espacial de origem, quando o pacote contém múltiplas camadas. |
| `_source_year` | Ano do arquivo ou versão de origem, usado para identificar e reprocessar ocorrências históricas. |
| `_source_feature_id` | Identificador sequencial da feição no arquivo espacial carregado. |
| `_source_member` | Membro/arquivo interno do pacote que originou o registro, quando aplicável. |
| `_source_srs_id` | Código EPSG/SRID da geometria recebida. |
| `_source_sha256` | Hash SHA-256 do artefato de origem usado na carga. |
| `_source_version` | Data ou rótulo da versão do artefato recebido. |
| `_batch_id` | Identificador do lote de processamento. |
| `_ingested_at` | Timestamp de ingestão do registro. |
| `_window` | Janela de carga (por exemplo, mês, ano ou snapshot) usada no processamento incremental. |
| `_window_year` | Ano da janela, usado para particionamento em tabelas com janelas mensais. |
| `_rescued_data` | Campos não reconhecidos pelo schema de leitura, quando o formato/conector os disponibiliza. |
| `geom_wkb` | Geometria serializada em Well-Known Binary; o SRID de origem é informado em `_source_srs_id` quando disponível. |

Nem todos esses campos existem em todas as tabelas. A composição exata do schema é definida no notebook de ingestão/transformação correspondente.

Campos padronizados e de controle nas tabelas Silver:

| Fonte / tabela | Colunas e definição |
|---|---|
| CEMADEN, `silver.cemaden_stations` | Identificação: `cod_estacao`, `cod_id_estacao`; nomes/localização: `nm_estacao`, `nm_cidade`, `cod_ibge`, `sg_uf`; coordenadas/altitude: `nr_latitude`, `nr_longitude`, `nr_altitude`; classificação: `cod_tipo_estacao`, `dc_tipo_estacao`, `cod_rede`, `sg_rede`; datas: `dt_instalacao`, `dt_hr_cadastro`, `dt_hr_inicio_inativo`, `dt_hr_ultima_remessa`, `_snapshot_date`; status: `fl_ativa`, `fl_no_cadastro_atual`; cotas: `nr_cota_atencao`, `nr_cota_alerta`, `nr_cota_transbordamento`; controle: `_ingested_at`. A estação permanece mesmo se ausente do cadastro atual. |
| CEMADEN, `silver.cemaden_rainfall_readings` | Identificação: `cod_estacao`, `cod_sensor`; tempo: `dt_hr_leitura_utc` (UTC), `dt_leitura`, `nr_hora`, `cod_dim_data` (fuso de São Paulo); chuva: `nr_chuva_mm`, `nr_chuva_mm_origem`, `fl_valor_invalido`; origem: `cd_qualificacao`, `ds_datahora_origem`; controle: `_window`, `_window_year`, `_source_file`, `_ingested_at`. Chuva negativa é inválida e vira nulo em `nr_chuva_mm`. |
| SIBH, `silver.sibh_stations` | Identificação: `cod_sensor`, `cod_estacao`, `cod_prefixo`, `cod_prefixo_alternativo`; cadastro: `nm_estacao`, `cod_tipo_sensor`, `dc_tipo_sensor`, `dc_grandeza`, `cod_instituicao`, `nm_instituicao`; operação/transmissão: `cd_situacao_operacao`, `fl_em_operacao`, `cd_situacao_transmissao`; localização: `nr_latitude`, `nr_longitude`, `cod_municipio_sibh`, `nm_municipio`, `cod_ibge`, `sg_ugrhi`, `nm_ugrhi`, `nm_sub_ugrhi`; atualização: `dt_hr_ultima_medicao`, `fl_no_cadastro_atual`, `_snapshot_date`, `_ingested_at`. Chuva e nível no mesmo local têm códigos de sensor distintos. |
| SIBH, `silver.sibh_measurements` | Identificação: `cod_sensor`, `cod_medicao`, `cod_tipo_sensor`, `dc_grandeza`; tempo: `dt_hr_medicao_utc` (UTC), `dt_medicao`, `nr_hora`, `cod_dim_data` (fuso de São Paulo); medida: `nr_valor`, `nr_valor_origem`, `nr_valor_lido`, `fl_valor_invalido`; origem: `cd_classificacao`, `ds_date_origem`; controle: `_window`, `_window_year`, `_source_file`, `_ingested_at`. Chuva negativa é inválida e vira nulo em `nr_valor`; nível mantém a unidade da fonte. |
| GeoSampa, `silver.geosampa_flooding_events` | `nr_id_ocorrencia`, `dt_ocorrencia`, `cod_dim_data`, `dc_tipo_ocorrencia`, `sg_subprefeitura`, `nm_subprefeitura`; localização `nr_x_utm`, `nr_y_utm` (EPSG:31983), `nr_latitude`, `nr_longitude` e `geom_wkb`; linhagem `_source_file`, `_source_layer`, `_source_year`, `_source_sha256`, `_source_srs_id`, `_ingested_at`. A identificação da ocorrência combina ano de origem e ID. |
| SGB, `silver.sgb_susceptibility_areas` | `cd_camada`, `dc_camada`, `nr_id_feicao`, `dc_classe`, `nr_grau_suscetibilidade`, `dc_processo`; geometria `geom_wkb` em EPSG:4674, `dc_tipo_geometria`, `nr_vertices`; medidas espaciais `nr_area_km2`, `nr_area_fonte_km2`, `nr_centroide_latitude`, `nr_centroide_longitude`, `nr_lat_min`, `nr_lat_max`, `nr_lon_min`, `nr_lon_max`; linhagem `_source_layer`, `_source_srs_id`, `_source_sha256`, `_source_version`, `_ingested_at`. |
| IBGE, `silver.ibge_census_tracts` | Setor e classificação: `cd_setor`, `dc_situacao`, `cd_situacao_detalhada`, `dc_situacao_detalhada`, `cd_tipo_setor`, `dc_tipo_setor`; território: `cd_municipio`, `nm_municipio`, `cd_distrito`, `nm_distrito`, `cd_subdistrito`, `nm_subdistrito`, `cd_bairro`, `nm_bairro`, `cd_favela`, `nm_favela`, `cd_nucleo_urbano`, `nm_nucleo_urbano`, `fl_favela`; população/domicílios: `nr_pessoas`, `nr_domicilios`, `nr_domicilios_particulares`, `nr_domicilios_coletivos`, `nr_media_moradores`, `nr_prop_domicilios_imputados`, `nr_domicilios_ocupados`; área: `nr_area_ibge_km2`, `nr_area_km2`, `nr_densidade_hab_km2`; geometria EPSG:4674 e derivados: `geom_wkb`, `dc_tipo_geometria`, `nr_vertices`, `nr_centroide_latitude`, `nr_centroide_longitude`, `nr_lat_min`, `nr_lat_max`, `nr_lon_min`, `nr_lon_max`; linhagem: `_source_srs_id`, `_source_sha256`, `_source_version`, `_ingested_at`. Variáveis V0001–V0007 são descritas no [guia do pipeline](README.md#ibge-censo-2022-por-setor-censitário). |
| OpenStreetMap, `silver.osm_roads` | Identificação/classificação: `nr_osm_id`, `cd_classe`, `cd_classe_geofabrik`, `nm_via`, `cd_referencia`; circulação: `cd_sentido`, `dc_sentido`, `fl_mao_unica`, `nr_velocidade_max_kmh`, `nr_nivel`; estrutura: `fl_ponte`, `fl_tunel`; geometria EPSG:4674: `geom_wkb`, `dc_tipo_geometria`, `nr_vertices`, `nr_centroide_latitude`, `nr_centroide_longitude`, `nr_lat_min`, `nr_lat_max`, `nr_lon_min`, `nr_lon_max`; medida: `nr_comprimento_km`; linhagem: `_source_feature_id`, `_source_srs_id`, `_source_sha256`, `_source_version`, `_ingested_at`. |
| INMET, `silver.inmet_stations` | `cod_estacao`, `nm_estacao`, `sg_uf`, `sg_regiao`, `nr_latitude`, `nr_longitude`, `nr_altitude_m`, `dt_fundacao`, `nr_anos_com_dados`, `nr_ultimo_ano`, `_ingested_at`. |
| INMET, `silver.inmet_weather_readings` | Identificação: `cod_estacao`; tempo: `dt_hr_medicao_utc` (UTC, fim da hora), `dt_hr_inicio_utc` (UTC), `dt_medicao`, `nr_hora`, `cod_dim_data` (fuso de São Paulo); medidas: `nr_chuva_mm`, `nr_pressao_mb`, `nr_pressao_max_mb`, `nr_pressao_min_mb`, `nr_radiacao_kj_m2`, `nr_temperatura_c`, `nr_temperatura_orvalho_c`, `nr_temperatura_max_c`, `nr_temperatura_min_c`, `nr_orvalho_max_c`, `nr_orvalho_min_c`, `nr_umidade_max_pct`, `nr_umidade_min_pct`, `nr_umidade_pct`, `nr_vento_direcao_graus`, `nr_vento_rajada_ms`, `nr_vento_velocidade_ms`; origem/controle: `ds_data_hora_origem`, `_window`, `_source_member`, `_ingested_at`. Valores `-9999` da origem viram nulo. |

## Objetos relacionados que não são tabelas

O pacote publicado ao site é gravado no **Volume** `fiap_analytics.gold.chuvasegura_web`; não é uma tabela Delta e, por isso, não está contado no inventário acima. O conteúdo e o contrato dos arquivos publicados estão em [`../app/DATA_CONTRACT.md`](../app/DATA_CONTRACT.md).

## Manutenção do dicionário

Ao adicionar ou alterar uma tabela:

1. atualize o inventário, a granularidade e a fonte nesta página;
2. descreva todos os campos novos, incluindo unidades, fuso, CRS, chaves e valores especiais;
3. mantenha os comentários de coluna/tabela nos DDL Gold e atualize a referência correspondente;
4. descreva explicitamente mudanças no schema de Bronze/Silver e nos contratos operacionais;
5. se a origem não define semanticamente um atributo bruto, registre a referência documental da fonte em vez de inventar uma definição.

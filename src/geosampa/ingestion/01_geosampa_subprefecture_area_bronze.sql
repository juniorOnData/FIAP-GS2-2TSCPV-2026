-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01_geosampa_subprefecture_area_bronze
-- MAGIC
-- MAGIC Recria a tabela de subprefeituras de São Paulo com os agrupamentos do Anexo IX (ata 41/14). A coluna `sg_subprefeitura_geosampa` traz a sigla usada nas ocorrências do GeoSampa.

-- COMMAND ----------

CREATE OR REPLACE TABLE fiap_analytics.bronze.geosampa_subprefecture_area (
  sg_subprefeitura          STRING    NOT NULL COMMENT 'Sigla da unidade conforme o Anexo IX',
  nm_subprefeitura          STRING    COMMENT 'Nome da unidade conforme o Anexo IX',
  ds_endereco               STRING    COMMENT 'Endereco da unidade conforme o Anexo IX',
  cd_agrupamento            STRING    COMMENT 'Agrupamento do Anexo IX (I, II, III ou IV)',
  tp_unidade                STRING    COMMENT 'SUBPREFEITURA ou SECRETARIA',
  sg_subprefeitura_geosampa STRING    COMMENT 'Sigla usada nas ocorrencias do GeoSampa (FO para FB); a Silver relaciona as ocorrencias por ela',
  ds_arquivo_origem         STRING    COMMENT 'Arquivo de origem dos dados',
  dt_carga                  TIMESTAMP COMMENT 'Data e hora da carga',
  CONSTRAINT pk_dim_subprefecture_area PRIMARY KEY (sg_subprefeitura)
)
COMMENT 'Dimensao de subprefeituras de Sao Paulo, com agrupamentos, a partir do Anexo IX (Relacao de Agrupamentos)';

-- COMMAND ----------

INSERT INTO fiap_analytics.bronze.geosampa_subprefecture_area
  SELECT
    v.sg_subprefeitura,
    v.nm_subprefeitura,
    v.ds_endereco,
    v.cd_agrupamento,
    v.tp_unidade,
    v.sg_subprefeitura_geosampa,
    'anexo_ix_ata_41-14_1468935749.pdf' AS ds_arquivo_origem,
    current_timestamp() AS dt_carga
  FROM
    VALUES
      ('AD', 'CIDADE ADEMAR', 'Rua Yervant Kissajikian, 416', 'II', 'SUBPREFEITURA', 'AD'),
      ('AF', 'ARICANDUVA/VILA FORMOSA', 'Rua Eponina, 82', 'IV', 'SUBPREFEITURA', 'AF'),
      ('BT', 'BUTANTÃ', 'Rua Ulpiano da Costa Manso, 201', 'III', 'SUBPREFEITURA', 'BT'),
      ('CL', 'CAMPO LIMPO', 'Av. Nossa Senhora do Bom Conselho, 59', 'III', 'SUBPREFEITURA', 'CL'),
      ('CS', 'CAPELA DO SOCORRO', 'Rua Cassiano dos Santos, 499', 'II', 'SUBPREFEITURA', 'CS'),
      ('CV', 'CASA VERDE', 'Rua Baroré, 284', 'I', 'SUBPREFEITURA', 'CV'),
      ('CT', 'CIDADE TIRADENTES', 'Estrada do Iguatemi, 2.751', 'IV', 'SUBPREFEITURA', 'CT'),
      ('EM', 'ERMELINO MATARAZZO', 'Av. São Miguel, 5977', 'IV', 'SUBPREFEITURA', 'EM'),
      ('FB', 'FREGUESIA/BRASILÂNDIA', 'Rua João Marcelino Branco, 95', 'I', 'SUBPREFEITURA', 'FO'),
      ('G', 'GUAIANAZES', 'Rua Prof. Cosme Deodato Tadeu, 136', 'IV', 'SUBPREFEITURA', 'G'),
      ('IP', 'IPIRANGA', 'Rua Lino Coutinho, 444', 'II', 'SUBPREFEITURA', 'IP'),
      ('IQ', 'ITAQUERA', 'Rua Gregório Ramalho, 103', 'IV', 'SUBPREFEITURA', 'IQ'),
      ('IT', 'ITAIM PAULISTA', 'Av. Marechal Tito, 3012', 'IV', 'SUBPREFEITURA', 'IT'),
      ('JA', 'JABAQUARA', 'Av. Eng. Armando de Arruda Pereira,2979', 'II', 'SUBPREFEITURA', 'JA'),
      ('JT', 'JAÇANÃ/TREMEMBÉ', 'Av. Luiz Stamatis, 300', 'I', 'SUBPREFEITURA', 'JT'),
      ('LA', 'LAPA', 'Rua Guaicurus, 1.000', 'III', 'SUBPREFEITURA', 'LA'),
      ('MB', 'M''BOI MIRIM', 'Estrada do Rivieira, 394', 'II', 'SUBPREFEITURA', 'MB'),
      ('MG', 'V.MARIA/V.GUILHERME', 'Rua General Mendes, 111', 'I', 'SUBPREFEITURA', 'MG'),
      ('MO', 'MOÓCA', 'Rua Taquari, 549', 'III', 'SUBPREFEITURA', 'MO'),
      (
        'MP',
        'SÃO MIGUEL PAULISTA',
        'Rua Ana Flora Pinheiro de Souza, 76',
        'IV',
        'SUBPREFEITURA',
        'MP'
      ),
      ('PA', 'PARELHEIROS', 'Av. Sadamus Inoue, 190', 'II', 'SUBPREFEITURA', 'PA'),
      ('PE', 'PENHA', 'Rua Candapui, 492', 'IV', 'SUBPREFEITURA', 'PE'),
      ('PI', 'PINHEIROS', 'Av. das Nações Unidas, 7123', 'III', 'SUBPREFEITURA', 'PI'),
      ('PJ', 'PIRITUBA/JARAGUÁ', 'Rua Luiz Carneiro, 193', 'I', 'SUBPREFEITURA', 'PJ'),
      ('PR', 'PERUS', 'Av. Ylidio Figueiredo, 349', 'I', 'SUBPREFEITURA', 'PR'),
      ('SA', 'SANTO AMARO', 'Praça Floriano Peixoto, 54', 'II', 'SUBPREFEITURA', 'SA'),
      ('SB', 'SAPOPEMBA', 'Estrada do Oratório, 172', 'IV', 'SUBPREFEITURA', 'SB'),
      ('SE', 'SÉ', 'Av. do Estado, 900', 'III', 'SUBPREFEITURA', 'SE'),
      ('SM', 'SÃO MATEUS', 'Rua Ragueb Chohfi, 1500', 'IV', 'SUBPREFEITURA', 'SM'),
      ('ST', 'SANTANA', 'Av. Tucuruvi, 808', 'I', 'SUBPREFEITURA', 'ST'),
      ('VM', 'VILA MARIANA', 'Rua José de Magalhães, 500', 'III', 'SUBPREFEITURA', 'VM'),
      ('VP', 'VILA PRUDENTE', 'Estrada do Oratório, 172', 'IV', 'SUBPREFEITURA', 'VP'),
      (
        'SPUA/NEC',
        'SECRETARIA DE COORDENAÇÃO DAS SUBPREFEITURAS',
        'Rua do Bosque, 1088',
        'III',
        'SECRETARIA',
        CAST(NULL AS STRING)
      ) AS v(
      sg_subprefeitura,
      nm_subprefeitura,
      ds_endereco,
      cd_agrupamento,
      tp_unidade,
      sg_subprefeitura_geosampa
    );

-- COMMAND ----------

SELECT
  *
FROM
  fiap_analytics.bronze.geosampa_subprefecture_area

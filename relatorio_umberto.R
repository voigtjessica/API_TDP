## importando bases em postgresql para R
# install.packages("RPostgreSQL")
# install.packages("odbc")

library(RPostgreSQL)
library(odbc)
library(dplyr)

#Conectando: creates a connection to the postgres database:

drv <- dbDriver("PostgreSQL")

# note that "con" will be used later in each connection to the database:

con <- odbc::dbConnect(drv,  dbname = "ebdb",
                 host = "aag6rh5j94aivq.cxbz7geveept.sa-east-1.rds.amazonaws.com", 
                 port = 5432,
                 user = "read_only_user",
                 password = "XXXXXXX")

#Descobrindo o encoding que sai a minha query e concertando isso:
dbGetQuery(con, "SHOW CLIENT_ENCODING")
postgresqlpqExec(con, "SET client_encoding = 'windows-1252'")

#Fazendo a query:
alertas_recebidos <- dbSendQuery(con, "SELECT i.id as id_inspecao, 
                                 i.project_id, p.city_id, 
                                 ct.name as municipality, 
                                 st.abbreviation as state 
                                 from inspections i 
                                 join projects p on p.id=i.project_id 
                                 join location_cities ct on ct.id=p.city_id 
                                 join location_states st on st.id=ct.state_id 
                                 where i.status!=6")

#Atribuindo o resultado a um objeto 
#Detalhe: a query só vai ficar armazenada na memória por um comando, por isso o 
#comando de impressão da query - dbFetch() - tem que ser executado logo depois

alertas <- dbFetch(alertas_recebidos)

#Agora vou subir a planilha do grupo controle:

library(googlesheets)
url_controle <- "https://docs.google.com/spreadsheets/d/1_AHohOe2wlAxB5_1jGP1pyGOHHyBcnCCvKTzVml5fNQ/edit?usp=sharing"

#Autenticação:
gs_ls() 

#Importando:
controle_sheet <- gs_title("grupo_controle_tdp")

#Atribuindo o df a um objeto:
controle_tdp <- controle_sheet %>%
  gs_read()

alertas_controle <- controle_tdp %>%
  inner_join(alertas, by = c("municipality", "state"))

#importando dados do Google Analytics que já haviam sido exportados para uma spreadsheet

url_acessos <- "https://docs.google.com/spreadsheets/d/19mQBrHcx5vXdEyC4MdFQSzEfPhBzc7KvQfDZaTdKjsw/edit?usp=sharing"
gs_ls() 
acessos_sheet <- gs_title("acessos_17dez_17jan")

#Atribuindo o df a um objeto:
acessos_tdp <- acessos_sheet %>%
  gs_read()

#Essa planilha não tem acento no nome das cidades, então eu preciso substituir
#os acentos na planilha do grupo controle:

remove_acento <- function(vec, Toupper=F) {
  vec <- tolower(vec)
  vec <- gsub('á', 'a', vec)
  vec <- gsub('ã', 'a', vec)
  vec <- gsub('à', 'a', vec)
  vec <- gsub('â', 'a', vec)
  vec <- gsub('é', 'e', vec)
  vec <- gsub('ê', 'e', vec)
  vec <- gsub('í', 'i', vec)
  vec <- gsub('ó', 'o', vec)
  vec <- gsub('ô', 'o', vec)
  vec <- gsub('õ', 'o', vec)
  vec <- gsub('ú', 'u', vec)
  vec <- gsub('ç', 'c', vec)
  vec <- gsub("'", '', vec)
  vec <- gsub("`", '', vec)
  #  vec <- gsub('\'', '', vec)
  if ( Toupper==T) vec <- toupper(vec)
  return(vec)
}

controle_tdp_semacento <- controle_tdp
controle_tdp_semacento$municipality <- remove_acento(controle_tdp_semacento$municipality)
acessos_tdp$municipality <- remove_acento(acessos_tdp$municipality)

acessos_controle <- controle_tdp_semacento %>%
  inner_join(acessos_tdp, by = c("municipality", "state"))

#not-sets:

acessos_not_set <- acessos_tdp %>%
  filter(municipality == "(not set)")

setwd("C:\\Users\\jvoig\\OneDrive\\Documentos\\planilhas\\tadepe\\relatorio_umberto")

write.table(acessos_controle, file="acessos_controle.csv",
            row.names = FALSE, fileEncoding = "utf-8",
            quote = FALSE, sep=",", na= "")

write.table(alertas_controle, file="alertas_controle.csv",
            row.names = FALSE, fileEncoding = "utf-8",
            quote = FALSE, sep=",", na= "")

write.table(acessos_not_set, file="acessos_not_set.csv",
            row.names = FALSE, fileEncoding = "utf-8",
            quote = FALSE, sep=",", na= "")

#dbClearResult(rs)
#dbDisconnect(con)

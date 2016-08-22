library(SSOAP)
library(XML)
library(RCurl)
library(xlsx)
library(data.table)
library(dplyr)

setwd("C:/R_SPE/Busca_Auto") 
#dados = read.csv("series.csv", sep = ";")


########### API BACEN
wsdl <- getURL("https://www3.bcb.gov.br/sgspub/JSP/sgsgeral/FachadaWSSGS.wsdl", ssl.verifypeer = FALSE)
doc <- xmlInternalTreeParse(wsdl)
def <- processWSDL(doc)
ff <- genSOAPClientInterface(def = def)

# Função getseries
getSeries <- function(codigos, data.ini = inicio, data.fim = fim, remove.old = TRUE) {
  xmlstr <- ff@functions$getValoresSeriesXML(codigos, data.ini, data.fim,
                                             .opts = list(ssl.verifypeer = FALSE))
  doc <- xmlInternalTreeParse(xmlstr)
  
  cleanup <- xpathApply(doc,"//SERIE", function(s) {
    id <- xmlGetAttr(s, "ID")
    s1 <- xmlSApply(s, function(x) xmlSApply(x, xmlValue))
    s1 <- t(s1)
    dimnames(s1) <- list(NULL, dimnames(s1)[[2]])
    df <- as.data.frame(s1, stringsAsFactors=FALSE)
    df$SERIE <- id
    df
  })
  df <- Reduce(rbind, cleanup)
  
  #df$data <- sapply(strsplit(df$DATA, "/"),
   #                          function(x) paste(c(x[2:1], 1), collapse="-"))
  
  df$data <- as.Date(df$DATA, format = "%d/%m/%Y")

  
  df$valor <- as.numeric(df$VALOR)
  df$serie <- factor(df$SERIE)
  
  if(remove.old){
    df$BLOQUEADO <- NULL
    df$SERIE <- NULL
    df$DATA <- NULL
    df$VALOR <- NULL
  }
  df
}

# Função ler dados
ler = function(s, r1, r2, c1, c2) {
  a = read.xlsx("Séries a Atualizar.xlsx", s, rowIndex = r1:r2, colIndex = c1:c2, header = FALSE)
  colnames(a) = c("serie", "id")
  a = na.omit(a)
  a
}


###################
# Baixa as séries #
###################

hoje <- Sys.Date()
inicio <- format(hoje - (365 * 1), "%d/%m/%Y")
fim <- format(hoje, "%d/%m/%Y")


# Series Diárias
dados12 = ler(2, 5, 390, 2, 3)
dados13 = ler(2, 5, 390, 4, 5)
dados14 = ler(2, 5, 390, 6, 7)
dados15 = ler(2, 5, 390, 8, 9)
dados16 = ler(2, 5, 390, 10, 11)


dados = mget(ls(pattern = "dad")) # Cria lista de variáveis começando com dad
dados = rbindlist(dados) # Cria grande data frame 
dados_id = dados
dados_id$serie = as.numeric(as.character(dados_id$serie))

dados = split(dados, rep(1:ceiling((nrow(dados)/50)),each=50)) # Divide data frame em listas de df com 50

# Busca series no BACEN
result = list()
for (i in 1:length(dados)) {
    result[[i]] <- getSeries(as.character(dados[[i]]$serie))
    
}

result = rbindlist(result) # converte a lista em um grande data frame
names(result) = c("atualizada", "valor", "serie")
result$status = 1
result$idusuario = 1

result$serie = as.numeric(as.character(result$serie))
result = left_join(result, dados_id)
result = result %>% select(atualizada, valor, status, idusuario, idserie = id)
result = result %>% filter(!is.na(valor))
result = result %>% filter(!is.na(atualizada))

# Remove dados temporarios
# lista = ls(pattern = "dad")
# rm(list = lista)


######################
library(DBI)

# MYSQL
library(RMySQL)
con <-  dbConnect(RMySQL::MySQL(),
                  username = "root",
                  password = "ASSEC2015/",
                  host = "127.0.0.1",
                  port = 3306,
                  dbname = "sgs_assec")

# Postgres
#library(RPostgreSQL)
# drv <- dbDriver("PostgreSQL")
# con <- dbConnect(drv, dbname = "sgs_assec",
#                  host = "10.209.8.170",
#                  port = 5432,
#                  user = "sgs_assec",
#                  password = "sgs_assec")

tabela = "serievalorteste2"


# Apaga os ultimos 3 anos
lista_delete = paste0("DELETE FROM ", tabela," WHERE atualizada >= '", format(hoje-(365*3), "%Y-%m-%d"), 
                      "' AND idserie in (", paste(unique(result$idserie), collapse = ","), ");")
res <- dbSendQuery(con, lista_delete)
res <- dbSendQuery(con, "commit;")
dbFetch(res)
dbClearResult(res)
res <- dbSendQuery(con, paste0("SELECT COUNT(*) FROM ", tabela))
dbFetch(res)


# Escreve na tabela
dbListTables(con)
dbWriteTable(con, tabela, result, overwrite = FALSE, row.names = FALSE, append = TRUE)
#dbWriteTable(con, tabela, result, overwrite = TRUE, row.names = FALSE, append = FALSE)
dbListFields(con, tabela)
res <- dbSendQuery(con, paste0("SELECT COUNT(*) FROM ", tabela, ";"))
dbFetch(res)
dbClearResult(res)




dbDisconnect(con)
# dbRemoveTable(con, "serievalorteste")

##################### DPLYR ###############################
# library(dplyr)
# library(RPostgreSQL)
# series2 <- tbl(src_postgres("sgs_assec",
#                                   host = "10.209.8.170",
#                                   port = 5432,
#                                   user = "sgs_assec",
#                                   password = "sgs_assec"), "serievalor")

# g = collect(series)
# collect(series) %>% ggplot(aes(valor)) + geom_histogram() + facet_wrap(~idserie, scales = "free")


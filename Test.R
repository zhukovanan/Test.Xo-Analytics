#Загружаем библиотеки
library(googlesheets4)
library(data.table)
library(magrittr)
library(lubridate)
#Загружаем данные
s.names <- c("transactions", "clients", "managers", "leads" ) #Определяем имена необходимых для загрузки листов
url <- c("https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ") #Определяем ссылку на файл

google.load <- function(x) { 
  assign(x, as.data.table(range_read(url, sheet = x)), envir = .GlobalEnv) # Строим вспомогательную функцию для массовой загрузки файлов
}

lapply(s.names , google.load ) #Неопсредственно загружаем данные

#Формирование таблицы для загрузки

setkey(leads, l_manager_id,l_client_id)
setkey(managers,manager_id)
leads[managers, on = c(l_manager_id = "manager_id"), `:=` (manager = i.d_manager, club = i.d_club )] #К данным по заявкам привязываем менеджера и клуб

leads[, trash_mark := ifelse(!leads$l_client_id %in% clients[, client_id , drop = FALSE], 1,0)] #Определяем мусорные заявки

trans_first <- transactions[ ,list(first_trans = min(created_at)) , by = l_client_id] #Первая покупка клиента
setkey(trans_first , l_client_id) # Задаем ключи


leads[trans_first,  #Присоединяем дату первой покупки по каждому клиенту 
      on = c(l_client_id = "l_client_id") , 
      `:=` (first_trans = i.first_trans ) ][clients,  #Присоединяем дату заявки по каждому клиенту
                                      on = c(l_client_id = "client_id"), 
                                      first_lead := i.created_at ]

#Заменяем неизветные значения дат
leads[is.na(first_trans)|is.na(first_lead) ,  `:=` ( first_trans = as.POSIXct("1900-10-10"), first_lead =  as.POSIXct("1900-10-10")) ]

k <- copy(leads)
k[, first_in_lead := min(created_at) , by = l_client_id] #Находим первую заявку в leads , 
                                                          #посколько мб временной лаг между появлением заявки и созданием client_id
k[, New_mark := dplyr::case_when (created_at <= first_trans & created_at <= first_lead & created_at <= first_in_lead ~ 1 
                                  , is.na(first_trans) & created_at <=  first_lead & created_at <= first_in_lead  ~ 1 , TRUE ~ 0)]

k[, New_client_return := dplyr::case_when((is.na(first_trans)| created_at < first_trans) & (created_at > first_in_lead| created_at > first_lead) & trash_mark != 1 ~ 1 , TRUE ~ 0) ]

monnb <- function(d) { lt <- as.POSIXlt(as.Date(d))
lt$year*12 + lt$mon } 
 mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }

k <- merge(k, transactions, by = "l_client_id", all = TRUE) #Удалить
k[!is.na(first_trans), Return_mark := ifelse( mondf(created_at.y, created_at.x) > 3, 1, 0) ]


la <- k[trash_mark == 0, list( created_at , l_client_id , Lead = 0, Value = 0)]
tr <- transactions[, list( created_at,l_client_id , Lead =  1, Value = m_real_amount)]
fin <- rbind(la,tr)
fin <- fin[order(l_client_id, created_at)][l_client_id %in% k$l_client_id] # Отсортируем по дате и оставим только тех клиентов , по которым были заявки
fin[,  `:=` (Previos = shift(Lead, 1) ,  Previos_dat = shift(created_at,1)) , by = l_client_id] #определяем разницу между предыдущей активностью и текущей
s <- fin[Lead == 0 & Previos == 1 & mondf(Previos_dat , created_at) >= 3]

k <- merge(k , s[, list(l_client_id ,created_at , Previos )], all.x = TRUE , by = c("l_client_id" , "created_at"))[, Previos := ifelse(is.na(Previos),0,Previos)]

#Размечаем заявки , которые в течение 7 дней привели к покупке
fin[, Potential_lead := sapply(created_at, function(x) sum(Lead[between(created_at, x +1, x + days(7))])), by = l_client_id]
fin[,Potentil_new := cumsum(shift(Lead, fill = 0)), by = l_client_id]
fin[, Money :=  sapply(created_at, function(x) sum(Value[between(created_at, x +1, x + days(7))])), by = l_client_id]

fin[,Potentil_new := cumsum(shift(Lead, fill = 0)), by = l_client_id][, Potentil_new := ifelse(Lead == 0 & Potentil_new < 1 &  Potential_lead > 0 , 1 , 0)]
fin <- fin[as.logical(Lead-shift(Lead, fill = Lead[1L]-1, type = "lead"))]
#Поскольку есть подряд идущие заявки , оставляем только те , которые непосредственно предшествовали транзакциям


k <- merge(k , fin[Lead == 0, list(Potential_lead,Money,Potentil_new)],  all.x = TRUE ,by =  c("l_client_id" , "created_at")) #Удалить
k[fin[Lead == 0], on = c("l_client_id" , "created_at"), `:=` (Potential_lead = i.Potential_lead, Money = i.Money , Potentil_new = i.Potentil_new) ]


#Final marks
final <-copy(k)[,c("d_utm_medium","l_manager_id","first_trans","first_lead","first_in_lead") := NULL][, Date := as.Date(created_at)]
vec <- c("d_utm_source", "manager" , "club" )
nam <- c("Potentil_new","Money" ,"Potential_lead")
final[, (vec) := (lapply(.SD, function(x) {x[is.na(x)] <- "Unknown" ; x})), .SDcols = vec]

final[, (nam) := (lapply(.SD, function(x) {x[is.na(x)] <- 0 ; x})), .SDcols = nam]

#Грузим в google doc 
gs4_create("Test" , sheets = final)

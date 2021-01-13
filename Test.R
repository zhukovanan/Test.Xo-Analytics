#Загружаем библиотеки

library(googlesheets4) #Для связывания с api google
library(data.table) # data manipulation
library(magrittr) #Построение pipe лайнов
library(lubridate) #Для удобной обработки дат

#Вспомогательные функции

google.load <- function(x) { 
  assign(x, as.data.table(range_read(url, sheet = x)), envir = .GlobalEnv) # Строим вспомогательную функцию для массовой загрузки файлов
}


monnb <- function(dat) { d <- as.POSIXlt(as.Date(dat)) 
                        d$year*12 + d$mon }  #Преобразование для расчета разницы в месяцах
mondf <- function(first, last) { monnb(last) - monnb(first) }  #Расчет разницы в месяцах между двумя датами

#Загружаем данные

s.names <- c("transactions", "clients", "managers", "leads" ) #Определяем имена необходимых для загрузки листов
url <- c("https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ") #Определяем ссылку на файл
lapply(s.names , google.load ) #Загрузка всех таблиц в глобальную среду

#Преобразование данных

leads[managers, on = c(l_manager_id = "manager_id"), `:=` (manager = i.d_manager, club = i.d_club )] #К данным по заявкам привязываем менеджера и клуб

leads[, trash_mark := ifelse(!leads$l_client_id %in% clients[, client_id , drop = FALSE], 1,0)] #Определяем мусорные заявки

trans_first <- transactions[ ,list(first_trans = min(created_at)) , by = l_client_id] #Первая покупка клиента

leads[trans_first,  #Присоединяем дату первой покупки по каждому клиенту 
      on = c(l_client_id = "l_client_id") , 
      `:=` (first_trans = i.first_trans ) ][clients,  #Присоединяем дату заявки по каждому клиенту
                                            on = c(l_client_id = "client_id"), 
                                            first_lead := i.created_at ]

leads[, first_in_lead := min(created_at) , by = l_client_id] #Находим первую заявку в leads

leads[, New_mark := dplyr::case_when (created_at <= first_trans & created_at <= first_lead & created_at <= first_in_lead ~ 1  #Размечаем новые заявки
                                  , is.na(first_trans) & created_at <=  first_lead & created_at <= first_in_lead  ~ 1 , TRUE ~ 0)]

leads[, New_client_return := dplyr::case_when((is.na(first_trans)| created_at < first_trans) #Размечаем заявки вернувшихся новых, которые не покупали
                                          & (created_at > first_in_lead| created_at > first_lead) & trash_mark != 1 ~ 1 , TRUE ~ 0) ]

#Создадим общую таблицу по активности пользователя 
#для простоты разметки заявок по следующим покупкам
la <- leads[trash_mark == 0, list( created_at , l_client_id , Lead = 0, Value = 0)] #Заявки
tr <- transactions[, list( created_at,l_client_id , Lead =  1, Value = m_real_amount)] #Транзакции

fin <- rbind(la,tr)[order(l_client_id, created_at)][l_client_id %in% leads$l_client_id] # Объединям все действия пользователей в один дата фрейм
                                                                                        #оставляем только клиентов , которые нас интересуют

fin[,  `:=` (Prev = shift(Lead, 1) ,  Prev_dat = shift(created_at,1)) , by = l_client_id] #определяем разницу между предыдущей активностью и текущей

s <- fin[Lead == 0 & Prev == 1 & mondf(Prev_dat , created_at) >= 3] #оставляем только те заявки , до кототой транзакция была не менее чем 3 месяца до

leads[s, on = c(l_client_id = "l_client_id", created_at = "created_at"), `:=` (Prev = i.Prev) ][, Prev := ifelse(is.na(Prev),0,Prev)] #размечаем заявки по return

#Размечаем заявки , которые в течение 7 дней привели к покупке

fin[, Potential_lead := sapply(created_at, function(x) sum(Lead[between(created_at, x +1 , x + days(7))])), by = l_client_id] #Считаем покупки после заявки в течение 7 дней


fin[, Money :=  sapply(created_at, function(x) sum(Value[between(created_at, x +1, x + days(7))])), by = l_client_id] #последующая сумма покупки по каждому действию

fin[,Potentil_new := cumsum(shift(Lead, fill = 0)), by = l_client_id][, Potentil_new :=   #Размечаем новых покупателей
                                                                     ifelse(Lead == 0 & Potentil_new < 1 &  Potential_lead > 0 , 1 , 0)]


fin <- fin %>%                      #Поскольку есть подряд идущие заявки , оставляем только те , которые непосредственно предшествовали транзакциям
  dplyr::group_by(l_client_id) %>%
  dplyr::filter(as.logical(Lead-shift(Lead, fill = -1 , type = "lead")))

#Размечаем заявки по покупкам
leads[setDT(fin)[Lead == 0], on = c("l_client_id" , "created_at"), `:=` (Potential_lead = i.Potential_lead, Money = i.Money , Potentil_new = i.Potentil_new) ]

leads[,c("d_utm_medium","l_manager_id","first_trans","first_lead","first_in_lead") := NULL][, Date := as.Date(created_at)] #Удаляем ненужные столбцы

vec <- c("d_utm_source", "manager" , "club" ) #Категориальные переменные для замены NA
nam <- c("Potentil_new","Money" ,"Potential_lead") #Количественные переменные для замены NA

leads[, (vec) := (lapply(.SD, function(x) {x[is.na(x)] <- "Unknown" ; x})), .SDcols = vec] #NA заменяем на неизвестный 

leads[, (nam) := (lapply(.SD, function(x) {x[is.na(x)] <- 0 ; x})), .SDcols = nam] #NA заменяем на 0

#Грузим в google doc 
gs4_create("Data" , sheets = leads)

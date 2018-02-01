#sumgroups

#version: b2 ver1

#Add
print(1:3)
print(6:7)
print(6)

#delete
b = 2

#change
print("19731221")












# remove target_date < 2017-11-01 00:00:00 in q 
# add saving directory






# pkg, wd, db, fun ---------------------------------------------------------------------------
pacman::p_load(RMySQL, pool, stringr, dplyr, lubridate, jsonlite)

setwd("~/Desktop")
wd_key = "~/Documents/Yuning Wang/Economic/Econ Modules-shiny"

agg_macro_keys<-fromJSON(file.path(wd_key,"dbkeys.json"))$localhost_agg_macro
agg_macro <- dbPool(
  drv = RMySQL::MySQL(),
  dbname = agg_macro_keys$dbname,
  host = agg_macro_keys$host,
  username = agg_macro_keys$id,
  password = agg_macro_keys$pw
)

q = function(sub, total){paste("select pro_id, mod_id, fred_id, transform, seas_adj, input_type, input_date, release_date, target_date, `value`
                                from (select * 
                               from agg_macro.estimates 
                               where provider = 'thawk' and transform = 'lev' and fred_id in ('", paste(sub, collapse = "','"), "')) a
                               right join (select pro_id as pro_s, mod_id as mod_s, input_type as type_s, input_date as input_s, release_date as release_s, target_date as target_s
                               from (select distinct pro_id, mod_id, input_type, input_date, release_date, target_date 
                               from agg_macro.estimates 
                               where provider = 'thawk' and transform = 'lev' and fred_id in ('", paste(sub, collapse = "','"), "')) a
                               left join (select distinct pro_id as pro, mod_id as `mod`, input_date as input, release_date as `release`, target_date  as target
                               from agg_macro.estimates 
                               where provider = 'thawk' and transform = 'lev' and fred_id in ('", paste(total, collapse = "','"), "') and (input_type like '%sumgroup') and target_date < '2017-11-01 00:00:00') b
                               on a.pro_id = b.pro
                               and a.mod_id = b.mod
                               and a.input_date = b.input
                               and a.release_date = b.release
                               and a.target_date = b.target
                               where b.target is null) b
                               on a.pro_id = b.pro_s
                               and a.mod_id = b.mod_s
                               and a.input_type = b.type_s
                               and a.input_date = b.input_s
                               and a.release_date = b.release_s
                               and a.target_date = b.target_s
                               order by a.pro_id, a.mod_id, a.fred_id, a.input_date, a.target_date;", sep = "")}
q_fit = function(total){paste("select * from agg_macro.estimates 
                              where fred_id = '", total, "' and transform = 'lev' and (input_type like '%sumgroup') and provider = 'thawk'", sep = "")} 
q_act = function(total){paste("select * from agg_macro.actuals 
                               where fred_id = '", total, "' and transform = 'lev' and target_date > '2010-01-01';", sep = "")}
tbl_db = function(sub, total){
  dt = dbGetQuery(agg_macro, q(sub, total))
  dt_agg = dt %>% group_by(pro_id, mod_id, transform, seas_adj, input_type, input_date, release_date, target_date) %>% summarise(value = sum(value, na.rm = T)) %>% ungroup() %>% mutate(fred_id = total, provider = "thawk", target_date = as.Date(target_date)) %>% 
    arrange(pro_id, mod_id, input_type, input_date, release_date, target_date) %>% select(pro_id, mod_id, fred_id, transform, seas_adj, input_type, provider, input_date, release_date, target_date, value)
  str_sub(dt_agg$input_type, start = -6, end = -1) <- "sumgroup"
  
  tbl_fit = dbGetQuery(agg_macro, q_fit(total)) %>% select(-est_id) %>% mutate(target_date = as.Date(target_date)) %>% bind_rows(., dt_agg) %>% unique()
  tbl_cpf = merge(x = dt_agg, y = (tbl_fit %>% mutate(target_date = target_date + months(1)) %>% select(-input_date, -release_date)), by.x = colnames(dt_agg %>% select(-value, -input_date, -release_date)), by.y = colnames(dt_agg %>% select(-value, -input_date, -release_date)), all.x = T) %>%
    mutate(value = value.x/value.y-1, transform = "mmp_cpf") %>% select(-value.x, -value.y)
  
  tbl_act = dbGetQuery(agg_macro, q_act(total)) %>% select(fred_id, target_date, value) %>% mutate(target_date = as.Date(target_date))
  tbl_cpa = merge(x = dt_agg, y = (tbl_act %>% mutate(target_date = target_date + months(1))), by.x = c("fred_id", "target_date"), by.y = c("fred_id", "target_date"), all.x = T) %>%
    mutate(value = value.x/value.y-1, transform = "mmp_cpa") %>% select(-value.x, -value.y)
  
  tbl_db = rbind(dt_agg, tbl_cpf, tbl_cpa)
  return(tbl_db)
}
##--------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------
sub_sa = c("RSMVPD", "RSFHFS", "RSEAS", "RSBMGESD", "RSDBS", "RSHPCS", "RSGASS", "RSCCAS", "RSSGHBMS", "RSGMS", "RSMSR", "RSNSR", "RSFSDP")
total_sa = "RSAFS"

sub_nsa = c("RSMVPDN", "RSFHFSN", "RSEASN", "RSBMGESDN", "RSDBSN", "RSHPCSN", "RSGASSN", "RSCCASN", "RSSGHBMSN", "RSGMSN", "RSMSRN", "RSNSRN", "RSFSDPN")
total_nsa = "RSAFSNA"

tbl_rsafs = tbl_db(sub =  sub_sa, total = total_sa)
tbl_rsafsna = tbl_db(sub = sub_nsa, total = total_nsa)
##--------------------------------------------------------------------------------------------

poolClose(agg_macro)


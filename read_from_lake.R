  
  library(httr)
  library(jsonlite)
  library(curl)
  
  # Setup
  profile_name="profile01"
  credentials_file="~/.ssh/credentials.json"
  datafile="data.csv"
  
  # Get credentials from config file
  config <- fromJSON(txt=credentials_file)
  
  client_id      <- config[[profile_name]]$client_id
  client_secret  <- config[[profile_name]]$client_secret
  tenant_id      <- config[[profile_name]]$tenant_id
  datastore_name <- config[[profile_name]]$datastore_name
  
  h <- new_handle()
  handle_setform(h,
                 "grant_type"="client_credentials",
                 "resource"="https://management.core.windows.net/",
                 "client_id"=client_id,
                 "client_secret"=client_secret
                 )
  
  auth_url=paste0("https://login.windows.net/", tenant_id, "/oauth2/token")
  
  req <- curl_fetch_memory(auth_url, handle = h)
  res <- fromJSON(rawToChar(req$content))
  
  my_url <- paste0("https://", datastore_name, ".azuredatalakestore.net/webhdfs/v1/?op=LISTSTATUS")
  
  r <- httr::GET(my_url,add_headers(Authorization = paste0("Bearer ", res$access_token)))
  
  my_url <- paste0("https://", datastore_name, ".azuredatalakestore.net/webhdfs/v1/", datafile, "?op=OPEN&read=true")
  
  dataRaw <- httr::GET(my_url, add_headers(Authorization = paste0("Bearer ", res$access_token)))
  
  writeBin(content(dataRaw), datafile)
  data <- read.csv(datafile)
  
  
  ## Clean up the Data
  pDate <- mdy(str_extract(data$date, '^.*\ '))
  pTime <- hms(paste0(str_extract(data$date, '\ .*$'),":00"))
  
  data <- within(data, pTime <- paste(pTime))
  
  data <- within(data, pDate <- paste(pDate))
  

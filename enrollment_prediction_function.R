

pred_enrl <- function(n_ahead=4, # number of terms ahead to predict
                      vday=0, # day of term at which to make predictions (VDAY 0 is first day)
                      server=NULL, # required: "PRGM", "PROD", or "PPRD"
                      JDBC.path=NULL, # path to jdbc jar file; file.choose if NULL
                      w.vday.only=FALSE,  # switch to exclude most recent info -- not working!
                      stat="head", # switch to "SCH" for credit hours
                      min_stat="sd") { # switch to "aic" for min AIC
  
  require(dplyr) # data manipulation
  require(forecast) # Arima functions
  require(getPass) # password masking 
  require(DBI) # database connectivity functions
  require(RJDBC) # JDBC Driver
  
  ##### Connect to Banner #####
  
  drv <- if(is.null(JDBC.path)) {
    
    print("Choose JDBC JAR File")
    
    JDBC(driverClass="oracle.jdbc.driver.OracleDriver", # Oracle thin driver name
         classPath = choose.files() ) # path to JDBC driver 
  } else {
    
    JDBC(driverClass="oracle.jdbc.driver.OracleDriver", # Oracle thin driver name
         classPath = JDBC.path ) # path to JDBC driver 
  }
  
  # make connection to Oracle database, given server name
  
  server.uri = switch(tolower(server), 
                      "prgm" = "jdbc:oracle:thin:@//oradev.admin.ad.cnm.edu/prgm.admin.ad.cnm.edu",
                      "prod" = "jdbc:oracle:thin:@//prod.admin.ad.cnm.edu/prod.admin.ad.cnm.edu",
                      "pprd" = "jdbc:oracle:thin:@//oradev.admin.ad.cnm.edu/pprd.admin.ad.cnm.edu")
  
  conn <- dbConnect(drv, 
                    url=server.uri, # from ITS 
                    user=getPass(msg="username: "), password=getPass()) # credentials
  
  ##### Get Banner Information #####
  
  # get information about terms
  terms <- dbGetQuery(conn = conn, statement = {
    
    "
    SELECT  DISTINCT
    terms.TERM_CODE
    , terms.TERM_SEQUENCE
    , terms.SEMESTER
    , rsts.SFRRSTS_START_DATE AS REG_START
    , terms.START_DATE
    , terms.CENSUS            AS CENSUS_DATE
    FROM    CHELINK.TERMS_AND_SEMESTERS terms
    JOIN    CHELINK.VDAY_SUMMARY vday
    ON   terms.TERM_CODE = vday.TERM_CODE
    JOIN    SATURN.SFRRSTS rsts
    ON  terms.TERM_CODE = rsts.SFRRSTS_TERM_CODE
    AND   rsts.SFRRSTS_PTRM_CODE IN ('1', '300')  
    AND   rsts.SFRRSTS_RSTS_CODE = 'RW'
    ORDER BY TERM_SEQUENCE
    "
    
  }) %>% 
    mutate_at(vars(SEMESTER), 
              funs(recode(., "F" = "Fall", "S" = "Spring", "R" = "Summer")))
  
  # get vday summary information (headcount, sch)
  vday_sum_raw <- dbGetQuery(conn = conn, statement = {
    
    "
    SELECT   vday.TERM_CODE   term
    ,   vday.DDAY        
    ,   vday.VDAY        
    ,   vday.RDAY        
    ,   vday.HEADCOUNT   AS head
    ,   vday.SCH_COUNT   AS sch
    FROM     CHELINK.VDAY_SUMMARY vday
    "
    
  }) %>%
    full_join(terms, by=c("TERM" = "TERM_CODE")) %>%
    mutate_at(vars(DDAY, REG_START, START_DATE, CENSUS_DATE), funs(base::as.Date(.))) %>%
    filter(DDAY >= REG_START, DDAY <= CENSUS_DATE) %>%
    mutate_at(vars(VDAY), funs(ifelse(. >= 1, .-1, .))) %>% # VDAY 1 => VDAY 0
    mutate_at(vars(TERM_SEQUENCE), funs(as.numeric(.))) %>%
    group_by(TERM) %>%
    mutate(SDAY = round(1- (VDAY / (min(VDAY))), 4)) %>% # calculate SDAY
    ungroup() %>%
    arrange(TERM, DDAY)
  
  ##### Fix VDAY_SUMMARY Errors #####
  
  vday_bounds <- vday_sum_raw %>%
    group_by(TERM) %>%
    summarize(
      min_vday = min(VDAY), 
      max_vday = max(VDAY))
  
  term_vdays <- do.call(rbind, lapply(vday_bounds %>% pull(TERM), function(term) {
    
    seq(
      from = vday_bounds %>% 
        filter(TERM == term) %>%
        pull(min_vday),
      to = vday_bounds %>% 
        filter(TERM == term) %>%
        pull(max_vday), by=1) %>%
      data.frame(TERM=term, vdays=.)
    
  })) %>% 
    mutate_at(vars(TERM), funs(as.character(.)))
  
  # fill in missing days with linear interpolation
  vday_sum <- term_vdays %>%
    left_join(vday_sum_raw, by=c("TERM", "vdays" = "VDAY")) %>%
    mutate_at(vars(TERM_SEQUENCE:CENSUS_DATE), 
              funs(ifelse(is.na(.), lag(.), .))) %>%
    mutate_at(vars(DDAY, RDAY), funs(ifelse(is.na(.), lag(.) + 1, .))) %>%
    mutate_at(vars(HEAD, SCH, SDAY), funs(ifelse(is.na(.), (lag(.) + lead(.)) / 2, .))) %>%
    mutate_at(vars(DDAY, REG_START, START_DATE, CENSUS_DATE), funs(as.Date)) %>%
    rename(VDAY = vdays)
  
  if(toupper(stat)=="SCH") {
    vday_sum <- vday_sum %>%
      select(-HEAD) %>%
      rename(HEAD=SCH) }
  
  # check for duplicates 
  dups <- vday_sum %>%
    group_by(TERM, VDAY) %>%
    summarize(n_vday = n()) %>%
    filter(n_vday > 1)
  
  if(nrow(dups) > 0) stop("Duplicate VDAY entries exist. Fix them first!")
  
  ##### Get Base Information ####
  
  voi=vday
  period <- ifelse(w.vday.only, 3, 4)
  term_lag <- ifelse(w.vday.only, 0, .5)
  
  # build basic information for all past / future terms
  n_years <- 30
  years <- rep(seq(2010, length.out=n_years), each=3)
  all_terms <- data.frame(TERM_SEQUENCE=seq(from=49, length.out=n_years*3),
                          AY=years,
                          YEAR=c(years[-c(1:2)], rep(max(years) + 1, 2)),
                          SEMESTER=rep(c("Fall", "Spring", "Summer"), n_years),
                          TERM_CODE_END=rep(c("70", "80", "90"), n_years)) %>%
    mutate(TERM_CODE = paste(AY, TERM_CODE_END, sep=""),
           TERM = paste(SEMESTER, YEAR))
  
  # get most up-to-date enrollment information 
  
  if(w.vday.only) vday_sum <- vday_sum %>% filter(VDAY == vday)
  
  info_max <- vday_sum %>% 
    filter(TERM == max(TERM)) %>%
    filter(VDAY == max(VDAY)) %>%
    select(TERM, VDAY, SEMESTER, TERM_SEQUENCE, HEAD) %>%
    mutate_at(vars(TERM_SEQUENCE), funs(ifelse(VDAY < 0, . - term_lag, 
                                               ifelse(VDAY > 0, . + term_lag, .))))
  
  # most advanced term with complete enrollment data given VDAY of interest
  max_w_vday <- vday_sum %>%
    filter(VDAY == voi) %>%
    filter(TERM == max(TERM)) %>%
    select(TERM, TERM_SEQUENCE, SEMESTER)
  
  # vday with most up-to-date enrollment information  
  test.day <- info_max %>% pull(VDAY)
  
  # create time series concatenating VDAY of interested (VOI) and test.day
  ts <- if(w.vday.only) {
    
    vday_sum %>%
      filter(VDAY == vday) %>% # vday of interest defined by user
      select(TERM, TERM_SEQUENCE, SEMESTER, VDAY, HEAD) %>%
      arrange(TERM_SEQUENCE) %>%
      mutate(head_diff1 = HEAD - lag(HEAD)) %>%
      slice(-1) %>% # remove NA row due to lagged differencing
      filter(TERM <= max_w_vday %>% pull(TERM))
    
  } else {
    
    vday_sum %>%
      filter(VDAY == test.day, SEMESTER == info_max %>% pull(SEMESTER)) %>%
      mutate_at(vars(TERM_SEQUENCE), funs(. - 0.5)) %>% # for ordering of series
      select(TERM, TERM_SEQUENCE, SEMESTER, VDAY, HEAD) %>%
      rbind(vday_sum %>%
              filter(VDAY == vday) %>% # vday of interest defined by user
              select(TERM, TERM_SEQUENCE, SEMESTER, VDAY, HEAD)) %>%
      arrange(TERM_SEQUENCE) %>%
      mutate(head_diff1 = HEAD - lag(HEAD)) %>%
      slice(-1) %>% # remove NA row due to lagged differencing
      filter(TERM <= max_w_vday %>% pull(TERM)) # terms with VOI only
    
  } 
  
  # create time series for current predictions
  
  ts.pred <- if(w.vday.only) {
    
    vday_sum %>%
      filter(VDAY == info_max %>% pull(VDAY)) %>% 
      select(TERM, TERM_SEQUENCE, SEMESTER, VDAY, HEAD) %>%
      arrange(TERM_SEQUENCE) %>%
      mutate(head_diff1 = HEAD - lag(HEAD)) %>%
      slice(-1) %>% mutate_at(vars(SEMESTER), funs(as.factor(.)))
    
  } else {
    
    vday_sum %>%
      filter(VDAY == info_max %>% pull(VDAY), SEMESTER == info_max %>% pull(SEMESTER)) %>%
      mutate_at(vars(TERM_SEQUENCE), funs(. - term_lag)) %>%
      select(TERM, TERM_SEQUENCE, SEMESTER, VDAY, HEAD) %>%
      rbind(vday_sum %>%
              filter(VDAY == vday) %>% # vday of interest
              select(TERM, TERM_SEQUENCE, SEMESTER, VDAY, HEAD)) %>%
      arrange(TERM_SEQUENCE) %>%
      mutate(head_diff1 = HEAD - lag(HEAD)) %>%
      slice(-1) %>%
      mutate_at(vars(SEMESTER), funs(as.factor(.)))
    
  } 
  
  ##### Get Best Models For Desired Predictions #####
  
  best_mods <- do.call(rbind,lapply(1:n_ahead, function(n_ahead) {
    
    print(n_ahead)
    base.ts <- ts
    
    # cut out intermediate times from predictions, if n_ahead > 3
    
    which_pred <- n_ahead
    
    if((!w.vday.only) & which_pred / 4 >= 1) which_pred=which_pred + which_pred %/% 4
    
    # get base time series allowing prediction for past 3 terms, depending on n_ahead
    
    i=0
    while(i < which_pred | 
          base.ts %>% slice(nrow(.)) %>% pull(VDAY) != test.day | 
          base.ts %>% slice(nrow(.)) %>% pull(SEMESTER) != info_max %>% pull(SEMESTER)) {
      
      base.ts <- base.ts %>%
        slice(-nrow(.))
      i <- i + 1
      
    }
    
    # list of order parameters to loop over (AR, Diff, MA)
    ogrid <- expand.grid(c(1, 2, 3), 1, c(1, 2, 3))
    
    all_os <-  do.call(rbind, lapply(1:nrow(ogrid), function(o_btw_seas) {
      
      do.call(rbind, lapply(1:nrow(ogrid), function(o_win_seas) {
        
        print(paste(o_btw_seas, o_win_seas))
        
        
        # fit base model; protect against failed ARMA fits: return NA
        base_fit <- tryCatch(Arima(y=ts.pred$head_diff1,
                                   order= ogrid %>% slice(o_btw_seas) %>% unlist(), 
                                   seasonal = list(order=ogrid %>% slice(o_win_seas) %>% unlist(), 
                                                   period=period)),
                             error=function(e) e)
        
        
        if(inherits(base_fit, "error")) {
          
          return(data.frame(dummy=TRUE, vday = test.day, o_btw = o_btw_seas, o_win = o_win_seas, 
                            b.aic=NA, aic=NA, term_seq= NA, lag=NA, 
                            bias_pct_pred=NA, bias_pct_act=NA, c.bias_pct_pred=NA, c.bias_pct_act=NA)) 
          
        }
        
        # get number of terms available for prediction
        n_terms <- base.ts %>%
          filter(SEMESTER == info_max %>% pull(SEMESTER), VDAY == test.day) %>% 
          summarize(n()) %>% 
          pull()
        
        if(test.day == voi ) n_terms <- floor(n_terms/2)
        
        
        xval_out <- data.frame(dummy=FALSE, vday=test.day, 
                               o_btw = o_btw_seas, o_win = o_win_seas,
                               b.aic=NA, aic=NA, term_seq=NA, lag=NA, 
                               bias_pct_pred=0, bias_pct_act=0, c.bias_pct_pred=0, c.bias_pct_act=0)
        
        #loop through terms and get bias
        # for(lag in 0:(n_terms - ceiling(n_terms/2.5))) {
        
        for(lag in 0:n_terms) {
          
          
          base_seq <- base.ts %>% summarize(max(TERM_SEQUENCE)) %>% pull() - lag * 3
          
          test.ts <- base.ts
          
          head_base <- test.ts %>% 
            filter(TERM_SEQUENCE == base_seq) %>%
            select(HEAD) %>%
            pull()
          
          # get actual headcount for term to be predicted
          head_actual <- ts %>% # actual headcount
            filter(TERM_SEQUENCE == base_seq + n_ahead - term_lag) %>%
            select(HEAD) %>% pull()
          
          if (length(head_base) == 0) next()
          
          fit <- tryCatch(Arima(y=test.ts$head_diff1, model = base_fit),
                          error=function(e) e)
          
          if(inherits(fit, "error")) if(inherits(fit, "error")) next()
          
          prior_error <- xval_out %>% 
            summarize(bias_mean = mean(bias_pct_pred, na.rm=TRUE)) %>% pull()
          
          preds <- cumsum(predict(fit, n.ahead=which_pred)$pred) + head_base
          pred <- preds[which_pred]
          bias_pct_pred <- (pred - head_actual) / pred
          bias_pct_act <- (pred - head_actual) / head_actual
          
          c.pred <- pred - (prior_error * pred)
          c.bias_pct_pred <- (c.pred - head_actual) /c.pred
          c.bias_pct_act <- (c.pred - head_actual) / head_actual
          
          xval_out[lag + 1, ] <- c(dummy=TRUE, 
                                   vday=test.day, o_btw = o_btw_seas, o_win = o_win_seas, 
                                   b.aic=base_fit$aicc, aic=fit$aicc, term_seq=base_seq + n_ahead - term_lag, lag=lag,
                                   bias_pct_pred, bias_pct_act, c.bias_pct_pred, c.bias_pct_act)
          
          if(nrow(xval_out) > 1) xval_out <- xval_out %>% filter(dummy==TRUE)
          
        }
        
        xval_out 
        
      }))
      
    })) %>% filter(!is.na(bias_pct_pred)) %>%
      select(-dummy)# end of all_os
    
    # get average and sd of error for all parameter combinations 
    o_stats_raw <- all_os %>%
      group_by(o_btw, o_win) %>%
      summarize(aic = max(aic), 
                bias_mean = mean(bias_pct_pred, na.rm=TRUE), 
                c.bias_mean=mean(c.bias_pct_pred, na.rm=TRUE), 
                bias_sd = sd(bias_pct_act, na.rm=TRUE),
                c.bias_sd = sd(c.bias_pct_act, na.rm=TRUE),
                n_terms = n(),
                min_term = min(term_seq)) %>%
      ungroup() 
    
    o_stats_raw <- if(tolower(min_stat) == "aic") {
      o_stats_raw %>% 
        filter(aic == min(aic, na.rm=TRUE))
    } else {
      o_stats_raw %>%
        filter(bias_sd == min(bias_sd, na.rm=TRUE))}
    
    o_stats <- o_stats_raw %>% 
      mutate(VDAY = test.day, n_ahead= n_ahead, 
             predict.with=base.ts %>% summarize(max(TERM)) %>% pull(), 
             predict.with.seq=base.ts %>% summarize(max(TERM_SEQUENCE)) %>% pull(),
             term.predicted=all_terms %>% 
               filter(TERM_SEQUENCE == info_max %>% 
                        pull(TERM_SEQUENCE) + n_ahead - term_lag) %>%
               pull(TERM),
             term.predicted.seq = all_terms %>% 
               filter(TERM_SEQUENCE == info_max %>% 
                        pull(TERM_SEQUENCE) + n_ahead - term_lag) %>%
               pull(TERM_SEQUENCE),
             semester.predicted = all_terms %>% 
               filter(TERM_SEQUENCE == info_max %>% 
                        pull(TERM_SEQUENCE) + n_ahead - term_lag) %>%
               pull(SEMESTER)) 
    
  }))  # end of best_mods functions
  
  ##### Forcast Enrollment #####
  
  # loop through terms-to-predict and use best models 
  preds_list <- do.call(rbind, lapply(1:n_ahead, function(pred_term) {
    
    # cut out intermediate times if n_ahead > 3 
    which_pred <- pred_term
    if((!w.vday.only) & which_pred / 4 >= 1) which_pred=which_pred + which_pred %/% 4
    
    # get optimal order parameters
    o_btw <- best_mods %>%
      filter(n_ahead == pred_term) %>%
      pull(o_btw)
    
    o_win <- best_mods %>%
      filter(n_ahead ==pred_term) %>%
      pull(o_win)
    
    ogrid <- expand.grid(c(1, 2, 3), 1, c(1, 2, 3))
    # refit model
    newfit <- Arima(y=ts.pred$head_diff1, 
                    order=ogrid %>% slice(o_btw) %>% unlist(), 
                    seasonal=list(order=ogrid %>% slice(o_win) %>% unlist(), period=4))
    
    # get predictions
    newpred <- cumsum(predict(newfit, n.ahead = which_pred)$pred) + 
      info_max %>% pull(HEAD)
    
    # get prediction 
    pred_out <- best_mods %>%
      filter(n_ahead == pred_term) %>%
      mutate(hc_pred = round(newpred[which_pred])) %>%
      left_join(ts.pred %>% filter(VDAY == voi) %>%
                  group_by(SEMESTER) %>%
                  filter(TERM_SEQUENCE == max(TERM_SEQUENCE)) %>%
                  select(hc_past=HEAD, SEMESTER), by=c("semester.predicted" = "SEMESTER")) %>%
      mutate(c.hc_pred = floor(hc_pred - c.bias_mean * hc_pred), 
             c.hc_pred_pct = (c.hc_pred - hc_past)/hc_past,
             p.975 = qt(.975, n_terms-1) * bias_sd / sqrt(n_terms),
             upper = c.hc_pred_pct + p.975, 
             lower = c.hc_pred_pct - p.975)
    
    return(pred_out)
    
  }))
  
  return(preds_list)
  
}


preds.hc <- pred_enrl(n_ahead = 4, vday=0, stat="head", min_stat="sd", server="prgm", 
                   JDBC.path = "C:/Users/tfarkas/Oracle/ojdbc6.jar")

preds.sch <- pred_enrl(n_ahead = 4, vday=0, stat="SCH", min_stat="sd", server="prgm", 
                       JDBC.path = "C:/Users/tfarkas/Oracle/ojdbc6.jar")


## make JDBC driver 
drv <- JDBC(driverClass="oracle.jdbc.driver.OracleDriver", # Oracle thin driver name
       classPath = "C:/Users/tfarkas/Oracle/ojdbc6.jar") # path to JDBC driver 

# connect to PROD as chelink
che_prod <- dbConnect(drv, 
                      url="jdbc:oracle:thin:@//prod.admin.ad.cnm.edu/prod.admin.ad.cnm.edu", # from ITS 
                      user="chelink", password=getPass()) # credentials

# dummy data frame
ogrid <- expand.grid(c(1, 2, 3), 1, c(1, 2, 3))

# create and write table to prod in chelink schema
dbWriteTable(conn=che_prod, name="ogrid", value=ogrid)
dbRemoveTable(conn=che_prod, name="ogrid")

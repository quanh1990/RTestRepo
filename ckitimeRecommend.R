# 10.6.105.37 R通过RJDBC连接HIVE
# 10.5.146.116 nohup /DATA/software/R-3.2.5/bin/R -f /DATA/hdfs/quanheng/prj_ckitime/ckitimeRecommend.R &
# /DATA/hdp/R/R-3.2.5/bin/R
options(scipen=200)


# 加载工具包

library(DBI)
library(rJava)
library(RJDBC)
library(stringr)
library(lubridate)

time_add <- function(timeStr,diff) {

	# timeStr : 2-23
	# diff : 2
	# return : 3-01
	
	timeStr_day <- as.numeric(strsplit(timeStr,'-')[[1]][1])
	timeStr_hour <- as.numeric(strsplit(timeStr,'-')[[1]][2])
	timeStr_hour_after <- timeStr_hour + diff
	
	if(timeStr_hour_after >= 24) timeStr_day <- timeStr_day + 1
	
	if(timeStr_hour_after < 0) timeStr_day <- timeStr_day - 1	
		
	timeStr_hour_after <- timeStr_hour_after %% 24
	
	if(nchar(timeStr_hour_after) == 1) timeStr_hour_after <- paste("0", timeStr_hour_after, sep = "")
	
	paste(timeStr_day, timeStr_hour_after, sep = '-')
}

# 初始化 classpath (works with hadoop 2.3.2)
#cp = c(
#       "/usr/hdp/2.3.2.0-2950/hive/lib/hive-jdbc-1.2.1.2.3.2.0-2950.jar",
#       "/usr/hdp/2.3.2.0-2950/hadoop/client/hadoop-common-2.7.1.2.3.2.0-2950.jar",
#       "/usr/hdp/2.3.2.0-2950/hive/lib/libthrift-0.9.2.jar",
#       "/usr/hdp/2.3.2.0-2950/hive/lib/hive-service-1.2.1.2.3.2.0-2950.jar",
#       "/usr/hdp/2.3.2.0-2950/hive/lib/httpclient-4.4.jar",
#       "/usr/hdp/2.3.2.0-2950/hive/lib/httpcore-4.4.jar",
#       "/usr/hdp/2.3.2.0-2950/hive/lib/hive-jdbc-1.2.1.2.3.2.0-2950-standalone.jar"
#)

cp = c(
	"/DATA/hdp/2.5.3.0-37/hive/lib/hive-jdbc-1.2.1000.2.5.3.0-37.jar",
	"/DATA/hdp/2.5.3.0-37/hadoop/client/hadoop-common-2.7.3.2.5.3.0-37.jar",
	"/DATA/hdp/2.5.3.0-37/hive/lib/libthrift-0.9.3.jar",
	"/DATA/hdp/2.5.3.0-37/hive/lib/hive-service-1.2.1000.2.5.3.0-37.jar",
	"/DATA/hdp/2.5.3.0-37/hive/lib/httpclient-4.4.jar",
	"/DATA/hdp/2.5.3.0-37/hive/lib/httpcore-4.4.jar",
	"/DATA/hdp/2.5.3.0-37/hive/lib/hive-jdbc-1.2.1000.2.5.3.0-37-standalone.jar"
)

.jinit(classpath = cp)

# 初始化 jdbc driver
drv <- JDBC(
	"org.apache.hive.jdbc.HiveDriver",
	"/DATA/hdp/2.5.3.0-37/hive/lib/hive-jdbc-1.2.1000.2.5.3.0-37.jar",
	identifier.quote="`"
)
conn <- dbConnect(drv, "jdbc:hive2://tr730l40-app.travelsky.com:10000/default", "hdfs", "")

cat("#### 读取值机数据...\n")
sql <- "select * from default.tmp_ckicheck"
data <- dbGetQuery(conn, sql)
#data <- read.csv("CA1291.csv", stringsAsFactors = FALSE)[,1:6]
#load("/DATA/quanheng/ckidata_30_day.rda")
cat("#### 修改列名...\n")
names(data) <- c("KEY","FLTTIME","FLTDATE","CKITIME","AIR_DEPT","PSRTYPE")
data <- data[!data$PSRTYPE %in% c("CIP","VIP","VIP#CIP"), ] # 去除VIP用户

cat("#### 读取近7天航班初始化数据...\n")
date_start <- Sys.Date() - 7
sql <- paste("select * from default.ds_ckiif_detail where pt >= '",date_start,"'", sep = '')
data_ckiif <- dbGetQuery(conn, sql)
data_ckiif$KEY <- paste(data_ckiif[,1],data_ckiif[,2],data_ckiif[,3], sep = '_')
names(data_ckiif) <- c("fltno","dept","dest","ckiiftime","ckiif","fltdate","KEY")

cat("#### 读取官网公布开放值机时间...\n")
sql <- "select * from default.tmp_ckitime_official"
data_ckitime_official <- dbGetQuery(conn, sql)
data_ckitime_official$air_dept <- paste(data_ckitime_official[,1],data_ckitime_official[,2], sep = '_')
names(data_ckitime_official) <- c("airline","dept","ckitime_official","air_dept")

#load("ckidata.rda")
#str(data)
# 时间刻度
ckitime_suggested <- c("1-00","1-01","1-02","1-03","1-04","1-05","1-06","1-07","1-08","1-09","1-10","1-11","1-12","1-13","1-14","1-15","1-16","1-17","1-18","1-19","1-20","1-21","1-22","1-23","2-00","2-01","2-02","2-03","2-04","2-05","2-06","2-07","2-08","2-09","2-10","2-11","2-12","2-13","2-14","2-15","2-16","2-17","2-18","2-19","2-20","2-21","2-22","2-23","3-00","3-01","3-02","3-03","3-04","3-05","3-06","3-07","3-08","3-09","3-10","3-11","3-12","3-13","3-14","3-15","3-16","3-17","3-18","3-19","3-20","3-21","3-22","3-23")

# 根据值机行为分析选取合适的数据时间段，使用第一个值机时间平均值作为推荐开放时间
# 颗粒度为航司-场站

# 初始化输出向量
{
	#air_dept_vector <- vector()
	fltno <- vector()
	dept <- vector()
	dest <- vector()
	ckitime_if_vec <- vector()
	ckitime_official_vec <- vector()
	#time_suggested <- vector()
	ckitime_suggested_first_median <- vector()
	ckitime_suggested_first_max <- vector()
	ckitime_suggested_first_final <- vector()
}

# 算法主体
air_dept_ca <- unique(data$AIR_DEPT)
air_dept_ca <- air_dept_ca[air_dept_ca != ""]
#air_dept_ca <- "CA_PEK"
for(air_dept in air_dept_ca) {

	#air_dept_vector <- c(air_dept_vector, air_dept)                        # 记录输出变量航司场站
	fltno_air_dept <- unique(data$KEY[which(data$AIR_DEPT == air_dept)])    # 当前航司场站下的所有航班号
	#fltno_air_dept <- "CA1857_PEK_SHA"
	data_air_dept <- data[which(data$AIR_DEPT == air_dept), ]               # 当前航司场站下的所有值机数据
	
	# 确定该航班的官方值机时间
	ckitime_official <- "3-24"
	
	if(paste("ALL_", strsplit(air_dept,"_")[[1]][2], sep = '') %in% data_ckitime_official$air_dept) {
		ckitime_official <- data_ckitime_official[data_ckitime_official$air_dept == paste("ALL_", strsplit(air_dept,"_")[[1]][2], sep = '') ,]$ckitime_official[1]
	}
	
	if(air_dept %in% data_ckitime_official$air_dept) {
		ckitime_official <- data_ckitime_official[data_ckitime_official$air_dept == air_dept,]$ckitime_official[1]
	} 
	
	for(fltno_key in fltno_air_dept) {
	
		# 确定该航班最近一周初始化时间的中位数
		ckiif <- "1-00"
		
		if(fltno_key %in% data_ckiif$KEY) {
			ckiif_vec <- sort(data_ckiif[data_ckiif$KEY == fltno_key,]$ckiif)
			ckiif <- ckiif_vec[ceiling(length(ckiif_vec)/2)]
		}
		
		# 过滤掉不符合要求的航班（扩展了国际航班，不再跳过）
		if(nchar(strsplit(fltno_key, '_')[[1]][1]) < 6) {
			cat(paste("processing : ", fltno_key, "跳过国际航班\n"), sep = "")
			#next
		}

		if(substr(strsplit(fltno_key, '_')[[1]][1],nchar(strsplit(fltno_key, '_')[[1]][1]), nchar(strsplit(fltno_key, '_')[[1]][1])) %in% LETTERS) {
			cat(paste("processing : ", fltno_key, "跳过后补航班\n"), sep = "")
			next
		}

		if(length(unique(data$FLTDATE[which(data$KEY == fltno_key)])) < 4) {
			cat(paste("processing : ", fltno_key, "一月内飞行次数低于4次\n"), sep = "")
			next
		}

		# 显示当前运行进度
		cat(paste("processing : " ,round(100 * which(air_dept_ca == air_dept)/length(air_dept_ca), 2), " % - ",
			round(100 * which(fltno_air_dept == fltno_key)/length(fltno_air_dept), 2), " %\n", sep = ""))

		# 存储航班号，航班始发地，航班目的地，推荐时间
		fltno <- c(fltno, strsplit(fltno_key,"_")[[1]][1])
		dept  <- c(dept, strsplit(fltno_key,"_")[[1]][2])
		dest  <- c(dest, strsplit(fltno_key,"_")[[1]][3])


		# 提取该航班的所有值机数据
		data_air_dept_fltno <- data_air_dept[data_air_dept$KEY == fltno_key, ]

		# 记录值机行为特征
		data_feature_perDay <- data.frame()

		for(flt_date in sort(unique(data_air_dept_fltno$FLTDATE))) {

			# 抽取当前航班当天数据
			data_air_dept_fltno_fltdate <- data_air_dept_fltno[which(data_air_dept_fltno$FLTDATE == flt_date),]

			# 抽取值机行为特征：存储每个时间间隔的值机人数
			d = seq(as.Date(as.Date(flt_date) - ddays(2)),as.Date(flt_date), by = "day")
			dates <- paste(d, "00:00:00", sep = " ")
			dates_vec <- vector()
			dates_vec_stamp <- vector()                                                              # 存储时间戳格式
			ckitime_stamp <- as.numeric(ymd_hms(data_air_dept_fltno_fltdate$CKITIME))-28800          # 时间戳转换有问题，需要减掉八个小时的偏移量 UTC问题
			ckinum_sep <- vector()

			# 把值机人数归到每个时间段内
			for(x in dates) {
				dates_vec <- c(dates_vec, as.character(strptime(x,"%Y-%m-%d %H:%M:%S") + 3600*0:23)) # 以1小时为间隔 一天一共24
				dates_vec_stamp <- c(dates_vec_stamp, strptime(x,"%Y-%m-%d %H:%M:%S") + 3600*0:23)
			}

			for(i in 1:length(dates_vec_stamp)) {
				time1 <- dates_vec_stamp[i]
				time2 <- time1 + 3600
				ckinum_sep <- c(ckinum_sep, length(ckitime_stamp[ckitime_stamp >= time1 & ckitime_stamp < time2]))
			}
			data_feature_perDay <- rbind(data_feature_perDay, ckinum_sep)
		}

		# 30天值机行为层次聚类分析
		dist.e = dist(data_feature_perDay, method = 'euclidean')
		model1 = hclust(dist.e, method = 'ward.D')

		# 聚类数选取为3
		result_cluster = cutree(model1, k = 3)

		# 从后遍历，纯度低于一定阈值则停止，目前阈值选取为0.7
		max_cluster <- names(sort(table(tail(result_cluster,ceiling(length(result_cluster)/3))), decreasing = TRUE)[1]) # 最后一个星期出现次数最多的类为标准
		per <- vector()
		for(i in ceiling(length(result_cluster)/3):length(result_cluster)) {
			per <- c(per, length(which(tail(result_cluster, i) == max_cluster))/length(tail(result_cluster, i)))
		}
		# 如果没有超过阈值的，就选后一半的日期
		if(length(which(per >= 0.7)) == 0 ) {
			fltno_key_date <- tail(sort(unique(data_air_dept_fltno$FLTDATE)), ceiling(length(result_cluster)/2))
		} else {
			fltno_key_date <- tail(sort(unique(data_air_dept_fltno$FLTDATE)), max(which(per >= 0.7)) + ceiling(length(result_cluster)/3) - 1)
		}

		# 第一个值机时间到起飞的时间间隔
		cki_first_to_fly <- vector()

		# 第一个初始化时间到第一个值机的时间间隔
		cki_if_to_cki <- vector()

		for(flt_date in sort(fltno_key_date)) {

			data_air_dept_fltno_fltdate <- data_air_dept_fltno[data_air_dept_fltno$FLTDATE == flt_date, ]


			# 记录第一个值机时间
			cki_first <- ymd_hms(min(data_air_dept_fltno_fltdate$CKITIME))

			# 拼接成完整的起飞时间 "2016-09-26 12:00:00 CST"
			if(unique(data_air_dept_fltno_fltdate$FLTTIME)[1] == "0:00:00") {
				flttime <- "00:00:00"
			} else {
				flttime <- data_air_dept_fltno_fltdate$FLTTIME[nchar(data_air_dept_fltno_fltdate$FLTTIME) == 8][1]
			}

			if(is.na(flttime))
				next

			flttime_new <- as.POSIXlt.date(paste(as.Date(flt_date), flttime, collapse = " "))

			if(flttime == "00:00:00") {
				#flttime_new <- ymd_hms(as.numeric(gsub("-","",flt_date)) * 1000000)
				flttime_new <- paste(flt_date, "00:00:00 UTC")
			} else {
				# CST 转换成  UTC 去掉空格冒号和“-”
				flttime_new <- ymd_hms(as.numeric(gsub("[-:[:space:]]","",flttime_new)))
			}

			cki_first_to_fly <- c(cki_first_to_fly, round(as.numeric(difftime(flttime_new, cki_first, units = "hours")), 2))
		}

		#ckitime_suggested_first_mean <- c(ckitime_suggested_first_mean, rev(ckitime_suggested)[24 - as.numeric(strsplit(flttime, ":")[[1]][1]) + ceiling(mean(sort(cki_first_to_fly)[c(-1,-length(cki_first_to_fly))]))])  # 平均值
		#ckitime_suggested_first_max <- c(ckitime_suggested_first_max, rev(ckitime_suggested)[24 - as.numeric(strsplit(flttime, ":")[[1]][1]) + ceiling(max(sort(cki_first_to_fly)[c(-1,-length(cki_first_to_fly))]))])  # 最大值
		ckitime_suggested_first_max_tmp <- rev(ckitime_suggested)[24 - as.numeric(strsplit(flttime, ":")[[1]][1]) + ceiling(max(sort(cki_first_to_fly)))]
		ckitime_suggested_first_median_tmp <- rev(ckitime_suggested)[24 - as.numeric(strsplit(flttime, ":")[[1]][1]) + ceiling(median(sort(cki_first_to_fly)))]

		if(length(ckitime_suggested_first_max_tmp) == 0 || nchar(ckitime_suggested_first_max_tmp) != 4)
			ckitime_suggested_first_max_tmp <- NA
		if(length(ckitime_suggested_first_median_tmp) == 0 || nchar(ckitime_suggested_first_median_tmp) != 4)
			ckitime_suggested_first_median_tmp <- NA
			
		# 根据初始化时间和官网时间
		
		if(is.na(ckitime_suggested_first_max_tmp) || is.na(ckitime_suggested_first_median_tmp) || ckiif >= ckitime_official) {
			ckitime_suggested_first_final_tmp <- time_add(ckiif,2)
		} else if(ckitime_suggested_first_max_tmp <= ckiif && ckitime_suggested_first_median_tmp >= ckitime_official) {
			ckitime_suggested_first_final_tmp <- min(time_add(ckiif,2),ckitime_official)
		} else if(ckitime_suggested_first_median_tmp <= ckitime_official) {
			ckitime_suggested_first_final_tmp <- ckitime_suggested_first_median_tmp
		} else if(ckitime_suggested_first_max_tmp <= ckitime_official) {
			ckitime_suggested_first_final_tmp <- ckitime_suggested_first_max_tmp
		} else {
			ckitime_suggested_first_final_tmp <- time_add(ckiif,2)
		}
		
		# 初始化时间晚于官网公布时间，可能是官网时间有误了
		if(!is.na(ckitime_suggested_first_max_tmp) && !is.na(ckitime_suggested_first_median_tmp) && ckiif > ckitime_official) {
			cat(paste("!!!! ",fltno_key," 初始化时间为 ",ckiif, ",晚于官方公布开放时间 ", ckitime_official, "\n",sep = ''))
			ckitime_suggested_first_final_tmp <- max(ckiif,ckitime_suggested_first_max_tmp)
		}
		
		ckitime_if_vec <- c(ckitime_if_vec, ckiif)
		ckitime_official_vec <- c(ckitime_official_vec, ckitime_official)
		ckitime_suggested_first_max <- c(ckitime_suggested_first_max, ckitime_suggested_first_max_tmp)  # 最大值
		ckitime_suggested_first_median <- c(ckitime_suggested_first_median, ckitime_suggested_first_median_tmp)  # 中位数
		ckitime_suggested_first_final <- c(ckitime_suggested_first_final, ckitime_suggested_first_final_tmp)
		#warnings()
	}

	# 在该航司场站所有航班的推荐开放值机时间中选取优势时刻作为航司场站的推荐开放值机时间
#       threshold <- sort(table(ckitime_suggested_first_mean), decreasing = TRUE)[1]/length(ckitime_suggested_first_mean)
#       i = 1
#       while(threshold < 0.5 & (i + 1) <= length(unique(ckitime_suggested_first_mean))) {
#               i <- i + 1
#
#               threshold <- sum(sort(table(ckitime_suggested_first_mean), decreasing = TRUE)[1:i])/length(ckitime_suggested_first_mean)
#       }
#
#       time_suggested <- c(time_suggested, min(names(sort(table(ckitime_suggested_first_mean), decreasing = TRUE)[1:i]))) # 取比重较大的时刻中最早的时刻
}

data_result <- data.frame(fltno = fltno, dept = dept, dest = dest, ckitime_suggested_first_max = ckitime_suggested_first_max, 
	ckitime_suggested_first_median = ckitime_suggested_first_median, ckitime_suggested_first_final = ckitime_suggested_first_final, 
	ckitime_if_vec = ckitime_if_vec, ckitime_official_vec = ckitime_official_vec,
	stringsAsFactors = FALSE)
data_result <- data_result[!is.na(data_result$ckitime_suggested_first_max), ]

write.table(data_result, file = "/DATA/hdfs/quanheng/prj_ckitime/ckitime_result.txt", row.names = FALSE, col.names = FALSE, quote = FALSE, sep = ",")



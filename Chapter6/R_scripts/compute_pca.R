#! /usr/bin/env Rscript
options(warn=-1)
#Establish connection to stdin for reading the data
con <- file("stdin","r")
#Read the data as a data frame
data <- read.table(con, header=FALSE, col.names=c("age", "amt", "asset", "transaction_amount", "service_rating", "product_rating", "current_stock", "payment_mode", "reward_points", "distance_to_store", "prod_bin_age", "cust_height"))
attach(data)
#Calculate covariance and correlation to understand the variation between the independent variables
covariance=cov(data, method=c("pearson"))
correlation=cor(data, method=c("pearson"))
#Calculate the principal components
pcdat=princomp(data)
summary(pcdat)
pcadata=prcomp(data, scale = TRUE)
pcadata

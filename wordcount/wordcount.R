#!/usr/bin/env Rscript

require('rmr2');
require('rhdfs');


hdfs.init()

## delete previous result if any
hdfs.rmr("/user/heytitle/wordcount/output")

## map function
map <- function(k,lines) {
  words.list <- strsplit(lines, '\\s')
  words <- unlist(words.list)
  return( keyval(words, 1) )
}

## reduce function
reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text",
            map=map, reduce=reduce)
}

## Submit job
hdfs.data <- '/user/heytitle/wordcount/data'
hdfs.out <- '/user/heytitle/wordcount/output'
out <- wordcount(hdfs.data, hdfs.out)

## Fetch results from HDFS
results <- from.dfs(out)

## check top 30 frequent words
results.df <- as.data.frame(results, stringsAsFactors=F) 
colnames(results.df) <- c('word', 'count') 
head(results.df[order(results.df$count, decreasing=T), ], 30)

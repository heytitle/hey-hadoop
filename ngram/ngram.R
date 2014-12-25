#!/usr/bin/env Rscript

require('rmr2');
require('rhdfs');
require('data.table');

hdfs.init()

rmr.options(backend="local")
project.root  <- "/Users/heytitle/Projects/map-reduce-hack/dataset/ngram"

#project.root   <- "/user/heytitle/ngram"
hdfs.data  <- paste( project.root, 'sample-data', sep='/' )
hdfs.out <- paste( project.root, 'output', sep='/')


## delete previous result if any
hdfs.rmr( hdfs.out )

## map function
# input: word year count books
map <- function(k,lines) {
  lines  <-strsplit( lines, '\\t' )
  keys   <- do.call( rbind, lines )[,1]
  values <- as.integer( do.call( rbind, lines)[,3] )
  return( keyval( keys, values ) )
}

## reduce function
reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

ngram <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text",
            map=map, reduce=reduce)
}

## Submit job
out <- ngram(hdfs.data, hdfs.out)

## Fetch results from HDFS
results <- from.dfs(out)

## check top 30 frequent words
results.df <- as.data.frame(results, stringsAsFactors=F)
colnames(results.df) <- c('word', 'count')
head(results.df[order(results.df$count, decreasing=T), ], 30)

#!/usr/bin/env Rscript

require('rmr2');
require('rhdfs');

hdfs.init()

#rmr.options(backend="local")
#project.root  <- "/Users/heytitle/Projects/map-reduce-hack/dataset/songs"

project.root   <- "/user/heytitle/songs"
hdfs.data  <- paste( project.root, 'data', sep='/' )
hdfs.out <- paste( project.root, 'output', sep='/')

## delete previous result if any
hdfs.rmr( hdfs.out )

## map function
# input: word year count books
map <- function(k,lines) {
    lines     <- strsplit( lines, '\\t' )
    years     <- do.call( rbind, lines )[,7]
    durations <- as.numeric( do.call( rbind, lines)[,5] )
    return ( keyval( years, durations ) )
}

## reduce function
reduce <- function( year, duration ) {
  keyval( year, sum(duration) )
}

song_duration <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text",
            map=map, reduce=reduce)
}

## Submit job
out <- song_duration( hdfs.data, hdfs.out )

## Fetch results from HDFS
results <- from.dfs(out)

## check top 30 frequent words
results.df <- as.data.frame(results, stringsAsFactors=F)
colnames(results.df) <- c('year', 'duration')
head(results.df[order(results.df$duration, decreasing=T ), ], 30)

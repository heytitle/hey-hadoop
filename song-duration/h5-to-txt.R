#!/usr/bin/env Rscript

require('rhdf5')

# Increase width range in order to prevent cat brokes a long line
# that is longer than `width` range
options( width = 150)

extract_file <- function( filename ) {

    data.analysis    <- h5read( filename, 'analysis' )
    data.metadata    <- h5read( filename, 'metadata' )
    data.musicbrainz <- h5read( filename, 'musicbrainz' );

    # Pick attributes from h5 file
    ## song_id
    ## song_name
    ## artist_id
    ## artist_name
    ## tempo
    ## duration
    ## year
    ## genre

    # Decorate data for
    genre <- data.metadata$songs$genre
    genre <- ifelse(  genre != '', genre, 'NA' )

    year  <- data.musicbrainz$songs$year
    year  <- ifelse( year != 0, year, 'NA' );

    selected_attribute <- c(
        data.metadata$songs$song_id,
        data.metadata$songs$title,
        data.metadata$songs$artist_id,
        data.metadata$songs$artist_name,
        data.analysis$songs$duration,
        data.analysis$songs$tempo,
        year,
        genre,
        '\n'
    )

    cat( selected_attribute, append=TRUE, file="music-data.txt", sep='\t' )
}

main <- function( ) {
    # Readfiles
    args<-commandArgs(TRUE)

    for( f in args ) {
        cat( 'Extracting', f, fill=TRUE )
        extract_file( f )
    }
}

main()

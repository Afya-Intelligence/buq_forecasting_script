# Base image https://hub.docker.com/u/rocker/
FROM rocker/r-base:latest

# Install git
#RUN apt-get -y update
#RUN apt-get -y install git

# system libraries of general use
## install debian packages
RUN apt-get update -qq && apt-get -y --no-install-recommends install \
libxml2-dev \
libcairo2-dev \
libsqlite3-dev \
libmariadbd-dev \
libpq-dev \
libssh2-1-dev \
unixodbc-dev \
libcurl4-openssl-dev \
libssl-dev \
git \
libmagick++-dev \
autoconf build-essential make


## update system libraries
RUN apt-get update && \
apt-get upgrade -y && \
apt-get clean

# copy necessary files
## renv.lock file
COPY ./renv.lock ./renv.lock


# install renv & restore packages

RUN Rscript -e 'install.packages("renv")' && Rscript -e 'options(timeout = 300); renv::restore()'



## app folder
COPY . ./script

# Copy Rprofile.site into the container's R home directory
# COPY Rprofile.site /usr/local/lib/R/etc/Rprofile.site




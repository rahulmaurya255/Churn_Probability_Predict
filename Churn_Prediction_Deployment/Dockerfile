FROM rocker/r-ver:4.1.0

RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libpq-dev unzip curl\
    && cd /tmp/ \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install && rm -rf /tmp/aws*
Run mkdir -p /var/secrets

RUN apt-get install gdal-bin proj-bin libgdal-dev libproj-dev -y
RUN apt-get install libudunits2-dev -y

RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev


RUN apt-get install gdal-bin proj-bin libgdal-dev libproj-dev -y
RUN apt-get install libudunits2-dev -y
RUN apt install clickhouse-client -y



#FROM python:3.11

#RUN pip install h2o
#RUN pip install pandas
#RUN pip install scikit-learn

RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*
  
RUN pip3 install pandas
RUN pip3 install numpy
RUN pip3 install clickhouse_driver
RUN pip3 install datetime
RUN pip3 install google-auth
RUN pip3 install gspread
RUN pip3 install oauth2client
RUN pip3 install pygeohash
RUN pip3 install folium

RUN mkdir -p /src/beckn
COPY ./beckn_etl /src/beckn
WORKDIR /src/beckn

RUN curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | tee /etc/apt/sources.list.d/clickhouse.list

# Update package lists
RUN apt-get update
RUN apt-get install clickhouse-client -y
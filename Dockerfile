FROM php:7.4-cli

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update -q && apt-get install -y --no-install-recommends \
        git \
        locales \
        unzip \
        unixodbc \
        unixodbc-dev \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

#php odbc
RUN docker-php-ext-configure pdo_odbc --with-pdo-odbc=unixODBC,/usr && \
    docker-php-ext-install pdo_odbc

#Exasol ODBC driver
RUN set -ex; \
    mkdir -p /tmp/exasol/odbc /opt/exasol ;\
    curl https://www.exasol.com/support/secure/attachment/153765/EXASOL_ODBC-7.1.rc1.tar.gz --output /tmp/exasol/odbc.tar.gz; \
    tar -xzf /tmp/exasol/odbc.tar.gz -C /tmp/exasol/odbc --strip-components 1; \
    cp /tmp/exasol/odbc/lib/linux/x86_64/libexaodbc-uo2214lv2.so /opt/exasol/;\
    echo "\n[exasol]\nDriver=/opt/exasol/libexaodbc-uo2214lv2.so\n" >> /etc/odbcinst.ini;\
    rm -rf /tmp/exasol;

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY . /code/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "/code/src/run.php"]

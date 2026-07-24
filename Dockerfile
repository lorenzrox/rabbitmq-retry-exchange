ARG RABBITMQ_VERSION=4.3.4

FROM rabbitmqdevenv/build-env-27.3 AS builder

WORKDIR /rabbitmq-server

RUN git clone --depth 1 --branch v${RABBITMQ_VERSION} \
    https://github.com/rabbitmq/rabbitmq-server.git .

COPY . deps/rabbitmq-retry-exchange/

RUN cp rabbitmq-components.mk deps/rabbitmq-retry-exchange/rabbitmq-components.mk

RUN make RABBITMQ_VERSION=v${RABBITMQ_VERSION} \
    -C deps/rabbitmq-retry-exchange \
    DIST_AS_EZS=yes dist

FROM rabbitmq:${RABBITMQ_VERSION}-management

COPY --from=builder \
    /rabbitmq-server/deps/rabbitmq-retry-exchange/plugins/rabbitmq_retry_exchange-v${RABBITMQ_VERSION}.ez \
    /plugins/

RUN rabbitmq-plugins enable --offline rabbitmq_retry_exchange
FROM rabbitmq:management

COPY dist/*.ez /plugins/

RUN rabbitmq-plugins enable --offline rabbitmq_retry_exchange
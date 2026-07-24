FROM rabbitmq:4.3.4-management

COPY dist/*.ez /plugins/

RUN rabbitmq-plugins enable --offline rabbitmq_retry_exchange
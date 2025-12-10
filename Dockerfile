FROM rabbitmq:4.2.1-management

COPY dist/*.ez /plugins/

RUN rabbitmq-plugins enable --offline rabbitmq_retry_exchange
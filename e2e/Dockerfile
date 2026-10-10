FROM trinodb/trino:483

USER root
COPY e2e/plugin/scent /usr/lib/trino/plugin/scent
# README deployment concept: the wrapper does Class.forName("io.trino.plugin.postgresql.PostgreSqlPlugin")
# from the scent plugin's classloader, so the delegate's MAIN jars are copied in — without their
# -services.jar, so the delegate stays registered only from its own (untouched) plugin dir.
RUN find /usr/lib/trino/plugin/postgresql -name '*.jar' ! -name '*-services.jar' \
        -exec cp -n {} /usr/lib/trino/plugin/scent/ \;
USER trino
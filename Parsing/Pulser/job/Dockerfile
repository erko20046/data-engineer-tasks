FROM docker.adata.kz:443/adata-data-engineers/stack/scraping/talend:9-b133-jre

ARG talend_job=Pulser_Job

ENV TALEND_JOB ${talend_job}
ENV ARGS ""

WORKDIR /opt/talend
COPY script/. .

RUN chmod +x ${TALEND_JOB}/${TALEND_JOB}_run.sh


CMD ["/bin/sh","-c","${TALEND_JOB}/${TALEND_JOB}_run.sh ${ARGS}"]


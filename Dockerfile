ARG VERSION

FROM postgres:$VERSION

ARG VERSION

RUN \
	if [ -z "${VERSION}" ] || [ ${VERSION%%.*} -ge 14 ]; then \
		apt update && \
		apt install -y postgresql-${VERSION%%.*}-pgvector && \
		rm -rf /var/lib/apt/lists/* \
	; fi

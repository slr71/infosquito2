FROM golang:1.26 AS build

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

ENV CGO_ENABLED=0

WORKDIR /src/infosquito2

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/infosquito2 .

FROM gcr.io/distroless/static-debian13:nonroot

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/infosquito2"
LABEL org.label-schema.version="$descriptive_version"

COPY --from=build /out/infosquito2 /bin/infosquito2

USER nonroot:nonroot

EXPOSE 60000
ENTRYPOINT ["/bin/infosquito2"]
CMD ["--help"]

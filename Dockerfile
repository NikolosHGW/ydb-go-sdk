FROM golang:1.21 AS builder
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin v1.59.1

WORKDIR /app
COPY . .

RUN golangci-lint run ./...

RUN go test -race -tags fast ./...




# FROM golang:1.21 AS builder

# # Установите golangci-lint
# RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin v1.59.1

# # Установите jq для обработки JSON
# RUN apt-get update && apt-get install -y jq && rm -rf /var/lib/apt/lists/*

# WORKDIR /app
# COPY . .

# # Запустите golangci-lint, подсчитайте ошибки и выведите их количество
# RUN golangci-lint run --out-format=json ./... | tee lint_output.json | jq '.Issues | length' > lint_errors_count.txt && \
#     echo "Total golangci-lint errors: $(cat lint_errors_count.txt)"

# # Вы можете добавить дополнительную проверку, чтобы прервать сборку при наличии ошибок
# # Например, если вы хотите прервать сборку, если есть хотя бы одна ошибка:
# RUN ERROR_COUNT=$(cat lint_errors_count.txt) && \
#     if [ "$ERROR_COUNT" -gt 0 ]; then \
#         echo "There are $ERROR_COUNT golangci-lint errors."; \
#         exit 1; \
#     fi

# # Запустите тесты
# RUN go test -race -tags fast ./...

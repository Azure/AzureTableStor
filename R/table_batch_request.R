#' Batch transactions for table storage
#'
#' @param endpoint A table storage endpoint, of class `table_endpoint`.
#' @param path The path component of the operation.
#' @param options A named list giving the query parameters for the operation.
#' @param headers A named list giving any additional HTTP headers to send to the host. AzureCosmosR will handle authentication details, so you don't have to specify these here.
#' @param body The request body for a PUT/POST/PATCH operation.
#' @param metadata The level of ODATA metadata to include in the response.
#' @param http_verb The HTTP verb (method) for the operation.
#' @param operations For `create_batch_transaction`, a list of individual operations to be batched up.
#' @param transaction For `do_batch_transaction`, an object of class `batch_transaction`.
#' @param operations A list of individual table operation objects, each of class `table_operation`.
#' @param batch_status_handler For `do_batch_transaction`, what to do if one or more of the batch operations fails. The default is to signal a warning and return a list of response objects, from which the details of the failure(s) can be determined. Set this to "pass" to ignore the failure.
#' @param num_retries The number of times to retry the call, if the response is a HTTP error 429 (too many requests). The Cosmos DB endpoint tends to be aggressive at rate-limiting requests, to maintain the desired level of latency. This will generally not affect calls to an endpoint provided by a storage account.
#' @param ... Arguments passed to lower-level functions.
#'
#' @details
#' Table storage supports batch transactions on entities that are in the same table and belong to the same partition group. Batch transactions are also known as _entity group transactions_.
#'
#' You can use `create_table_operation` to produce an object corresponding to a single table storage operation, such as inserting, deleting or updating an entity. Multiple such objects can then be passed to `create_batch_transaction`, which bundles them into a single atomic transaction. Call `do_batch_transaction` to send the transaction to the endpoint.
#'
#' Note that batch transactions are subject to some limitations imposed by the REST API:
#' - All entities subject to operations as part of the transaction must have the same `PartitionKey` value.
#' - An entity can appear only once in the transaction, and only one operation may be performed against it.
#' - The transaction can include at most 100 entities, and its total payload may be no more than 4 MB in size.
#'
#' @return
#' `create_table_operation` returns an object of class `table_operation`.
#'
#' Assuming the batch transaction did not fail due to rate-limiting, `do_batch_transaction` returns a list of objects of class `table_operation_response`, representing the results of each individual operation. Each object contains elements named `status`, `headers` and `body` containing the respective parts of the response. Note that the number of returned objects may be smaller than the number of operations in the batch, if the transaction failed.
#' @seealso
#' [import_table_entities], which uses (multiple) batch transactions under the hood
#'
#' [Performing entity group transactions](https://docs.microsoft.com/en-us/rest/api/storageservices/performing-entity-group-transactions)
#' @examples
#' \dontrun{
#'
#' endp <- table_endpoint("https://mycosmosdb.table.cosmos.azure.com:443", key="mykey")
#' tab <- create_storage_table(endp, "mytable")
#'
#' ## a simple batch insert
#' ir <- subset(iris, Species == "setosa")
#'
#' # property names must be valid C# variable names
#' names(ir) <- sub("\\.", "_", names(ir))
#'
#' # create the PartitionKey and RowKey properties
#' ir$PartitionKey <- ir$Species
#' ir$RowKey <- sprintf("%03d", seq_len(nrow(ir)))
#'
#' # generate the array of insert operations: 1 per row
#' ops <- lapply(seq_len(nrow(ir)), function(i)
#'     create_table_operation(endp, "mytable", body=ir[i, ], http_verb="POST")))
#'
#' # create a batch transaction and send it to the endpoint
#' bat <- create_batch_transaction(endp, ops)
#' do_batch_transaction(bat)
#'
#' }
#' @rdname table_batch
#' @export
create_table_operation <- function(endpoint, path, options=list(), headers=list(), body=NULL,
    metadata=c("none", "minimal", "full"), http_verb=c("GET", "PUT", "POST", "PATCH", "DELETE", "HEAD"))
{
    headers <- utils::modifyList(headers, list(DataServiceVersion="3.0;NetFx"))
    if(!is.null(metadata))
    {
        accept <- switch(match.arg(metadata),
            "none"="application/json;odata=nometadata",
            "minimal"="application/json;odata=minimalmetadata",
            "full"="application/json;odata=fullmetadata")
        headers$Accept <- accept
    }

    obj <- list()
    obj$endpoint <- endpoint
    obj$path <- path
    obj$options <- options
    obj$headers <- headers
    obj$method <- match.arg(http_verb)
    obj$body <- body
    structure(obj, class="table_operation")
}


serialize_table_operation <- function(object)
{
    UseMethod("serialize_table_operation")
}


serialize_table_operation.table_operation <- function(object)
{
    url <- httr::parse_url(object$endpoint$url)
    url$path <- object$path
    url$query <- object$options

    preamble <- c(
        "Content-Type: application/http",
        "Content-Transfer-Encoding: binary",
        "",
        paste(object$method, httr::build_url(url), "HTTP/1.1"),
        paste0(names(object$headers), ": ", object$headers),
        if(!is.null(object$body)) "Content-Type: application/json"
    )

    if(is.null(object$body))
        preamble
    else if(!is.character(object$body))
    {
        body <- jsonlite::toJSON(object$body, auto_unbox=TRUE, null="null")
        # special-case treatment for 1-row dataframes
        if(is.data.frame(object$body) && nrow(object$body) == 1)
            body <- substr(body, 2, nchar(body) - 1)
        c(preamble, "", body)
    }
    else c(preamble, "", object$body)
}


#' @rdname table_batch
#' @export
create_batch_transaction <- function(endpoint, operations)
{
    structure(list(endpoint=endpoint, ops=operations), class="batch_transaction")
}


#' @rdname table_batch
#' @export
do_batch_transaction <- function(transaction, ...)
{
    UseMethod("do_batch_transaction")
}


#' @rdname table_batch
#' @export
do_batch_transaction.batch_transaction <- function(transaction,
    batch_status_handler=c("warn", "stop", "message", "pass"), num_retries=10, ...)
{
    # batch REST API only supports 1 changeset per batch, and is unlikely to change
    batch_bound <- paste0("batch_", uuid::UUIDgenerate())
    changeset_bound <- paste0("changeset_", uuid::UUIDgenerate())
    headers <- list(`Content-Type`=paste0("multipart/mixed; boundary=", batch_bound))

    batch_preamble <- c(
        paste0("--", batch_bound),
        paste0("Content-Type: multipart/mixed; boundary=", changeset_bound),
        ""
    )
    batch_postscript <- c(
        "",
        paste0("--", changeset_bound, "--"),
        paste0("--", batch_bound, "--")
    )
    serialized <- lapply(transaction$ops,
        function(op) c(paste0("--", changeset_bound), serialize_table_operation(op)))
    body <- paste0(c(batch_preamble, unlist(serialized), batch_postscript), collapse="\n")
    if(nchar(body) > 4194304)
        stop("Batch request too large, must be 4MB or less")

    for(i in seq_len(num_retries))
    {
        res <- call_table_endpoint(transaction$endpoint, "$batch", headers=headers, body=body, encode="raw",
            http_verb="POST")
        reslst <- process_batch_response(res)
        statuses <- sapply(reslst, `[[`, "status")
        complete <- all(statuses != 429)
        if(complete)
            break
        Sys.sleep(1.5^i)
    }
    if(!complete)
        httr::stop_for_status(429, "complete batch transaction")
    batch_status_handler <- match.arg(batch_status_handler)
    if(any(statuses >= 300) && batch_status_handler != "pass")
    {
        msg <- paste("Batch transaction failed, max status code was", max(statuses))
        switch(batch_status_handler,
            "stop"=stop(msg, call.=FALSE),
            "warn"=warning(msg, call.=FALSE),
            "message"=message(msg, call.=FALSE)
        )
    }
    statuses
}


process_batch_response <- function(response)
{
    # assume response (including body) is always text
    response <- rawToChar(response)
    lines <- strsplit(response, "\r?\n\r?")[[1]]
    batch_bound <- lines[1]
    changeset_bound <- sub("^.+boundary=(.+)$", "\\1", lines[2])
    n <- length(lines)

    # assume only 1 changeset
    batch_end <- grepl(batch_bound, lines[n])
    if(!any(batch_end))
        stop("Invalid batch response, batch boundary not found", call.=FALSE)
    changeset_end <- grepl(changeset_bound, lines[n-1])
    if(!any(changeset_end))
        stop("Invalid batch response, changeset boundary not found", call.=FALSE)

    lines <- lines[3:(n-3)]
    op_bounds <- grep(changeset_bound, lines)
    Map(
        function(start, end) process_operation_response(lines[seq(start, end)]),
        op_bounds + 1,
        c(op_bounds[-1], length(lines))
    )
}


process_operation_response <- function(response)
{
    blanks <- which(response == "")
    if(length(blanks) < 2)
        stop("Invalid operation response", call.=FALSE)

    headers <- response[seq(blanks[1]+1, blanks[2]-1)]  # skip over http stuff

    status <- as.numeric(sub("^.+ (\\d{3}) .+$", "\\1", headers[1]))
    headers <- strsplit(headers[-1], ": ")
    names(headers) <- sapply(headers, `[[`, 1)
    headers <- sapply(headers, `[[`, 2, simplify=FALSE)
    class(headers) <- c("insensitive", "list")

    body <- if(!(status %in% c(204, 205)) && blanks[2] < length(response))
        response[seq(blanks[2]+1, length(response))]
    else NULL

    obj <- list(status=status, headers=headers, body=body)
    class(obj) <- "table_operation_response"
    obj
}


#' @export
print.table_operation <- function(x, ...)
{
    cat("<Table storage batch operation>\n")
    invisible(x)
}

#' @export
print.table_operation_response <- function(x, ...)
{
    cat("<Table storage batch operation response>\n")
    invisible(x)
}

#' @export
print.batch_transaction <- function(x, ...)
{
    cat("<Table storage batch transaction>\n")
    invisible(x)
}

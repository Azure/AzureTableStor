#' Table storage endpoint
#'
#' Table storage endpoint object, and method to call it.
#'
#' @param endpoint For `table_endpoint`, the URL of the table service endpoint. This will be of the form `https://{account-name}.table.core.windows.net` if the service is provided by a storage account in the Azure public cloud, while for a CosmosDB database, it will be of the form `https://{account-name}.table.cosmos.azure.com:443`. For `call_table_endpoint`, an object of class `table_endpoint`.
#' @param key The access key for the storage account.
#' @param token An Azure Active Directory (AAD) authentication token. For compatibility with AzureStor; not used for table storage.
#' @param sas A shared access signature (SAS) for the account. At least one of `key` or `sas` should be provided.
#' @param api_version The storage API version to use when interacting with the host. Defaults to "2019-07-07".
#' @param path For `call_table_endpoint`, the path component of the endpoint call.
#' @param options For `call_table_endpoint`, a named list giving the query parameters for the operation.
#' @param headers For `call_table_endpoint`, a named list giving any additional HTTP headers to send to the host. AzureCosmosR will handle authentication details, so you don't have to specify these here.
#' @param body For `call_table_endpoint`, the request body for a PUT/POST/PATCH call.
#' @param http_verb For `call_table_endpoint`, the HTTP verb (method) of the operation.
#' @param http_status_handler For `call_table_endpoint`, the R handler for the HTTP status code of the response. ``"stop"``, ``"warn"`` or ``"message"`` will call the corresponding handlers in httr, while ``"pass"`` ignores the status code. The latter is primarily useful for debugging purposes.
#' @param return_headers For `call_table_endpoint`, whether to return the (parsed) response headers instead of the body. Ignored if `http_status_handler="pass"`.
#' @param metadata For `call_table_endpoint`, the level of ODATA metadata to include in the response.
#' @param num_retries The number of times to retry the call, if the response is a HTTP error 429 (too many requests). The Cosmos DB endpoint tends to be aggressive at rate-limiting requests, to maintain the desired level of latency. This will generally not affect calls to an endpoint provided by a storage account.
#' @param ... For `call_table_endpoint`, further arguments passed to `AzureStor::call_storage_endpoint` and `httr::VERB`.
#'
#' @return
#' `table_endpoint` returns an object of class `table_endpoint`, inheriting from `storage_endpoint`. This is the analogue of the `blob_endpoint`, `file_endpoint` and `adls_endpoint` classes provided by the AzureStor package.
#'
#' `call_table_endpoint` returns the body of the response by default, or the headers if `return_headers=TRUE`. If `http_status_handler="pass"`, it returns the entire response object without modification.
#'
#' @seealso
#' [storage_table], [table_entity], [AzureStor::call_storage_endpoint]
#'
#' [Table service REST API reference](https://docs.microsoft.com/en-us/rest/api/storageservices/table-service-rest-api)
#'
#' [Authorizing requests to Azure storage services](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-requests-to-azure-storage)
#' @examples
#' \dontrun{
#'
#' # storage account table endpoint
#' table_endpoint("https://mystorageacct.table.core.windows.net", key="mykey")
#'
#' # Cosmos DB table endpoint
#' table_endpoint("https://mycosmosdb.table.cosmos.azure.com:443", key="mykey")
#'
#' }
#' @rdname table_endpoint
#' @export
table_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                           api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "table"))
        warning("Not a recognised table endpoint", call.=FALSE)
    if(!is.null(token))
    {
        warning("Table storage does not use Azure Active Directory authentication")
        token <- NULL
    }
    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("table_endpoint", "storage_endpoint")
    obj
}


#' @rdname table_endpoint
#' @export
call_table_endpoint <- function(endpoint, path, options=list(), headers=list(), body=NULL, ...,
    http_verb=c("GET", "DELETE", "PUT", "POST", "HEAD", "PATCH"),
    http_status_handler=c("stop", "warn", "message", "pass"),
    return_headers=(http_verb == "HEAD"),
    metadata=c("none", "minimal", "full"),
    num_retries=10)
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

    if(is.list(body))
    {
        body <- jsonlite::toJSON(body, auto_unbox=TRUE, null="null")
        headers$`Content-Length` <- nchar(body)
        headers$`Content-Type` <- "application/json"
    }

    http_verb <- match.arg(http_verb)
    # handle possible rate limiting in Cosmos DB
    for(i in seq_len(num_retries))
    {
        res <- call_storage_endpoint(endpoint, path=path, options=options, body=body, headers=headers,
            http_verb=http_verb, http_status_handler="pass", return_headers=return_headers, ...)
        if(httr::status_code(res) != 429)
            break
        Sys.sleep(1.5^i)
    }
    process_storage_response(res, match.arg(http_status_handler), FALSE)
}


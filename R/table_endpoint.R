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
#' @param metadata For `call_table_endpoint`, the level of ODATA metadata to include in the response.
#' @param ... For `call_table_endpoint`, further arguments passed to `AzureStor::call_storage_endpoint` and `httr::VERB`.
#'
#' @return
#' An object of class `table_endpoint`, inheriting from `storage_endpoint`. This is the analogue of the `blob_endpoint`, `file_endpoint` and `adls_endpoint` classes provided by the AzureStor package.
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
    metadata=c("none", "minimal", "full"),
    http_status_handler=c("stop", "warn", "message", "pass"),
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

    # handle possible rate limiting in Cosmos DB
    for(i in seq_len(num_retries))
    {
        res <- call_storage_endpoint(endpoint, path=path, options=options, body=body, headers=headers,
            http_status_handler="pass", ...)
        if(httr::status_code(res) != 429)
            break
        Sys.sleep(1.5^i + runif(1, max=0.1))
    }
    process_storage_response(res, match.arg(http_status_handler), FALSE)
}


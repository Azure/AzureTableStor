#' @import AzureRMR
#' @import AzureStor
NULL

utils::globalVariables(c("self", "private"))

.onLoad <- function(libname, pkgname)
{
    AzureStor::az_storage$set("public", "get_table_endpoint", overwrite=TRUE,
    function(key=self$list_keys()[1], sas=NULL, token=NULL)
    {
        table_endpoint(self$properties$primaryEndpoints$table, key=key, sas=sas, token=token)
    })
}


# assorted imports of friend functions
sign_sha256 <- get("sign_sha256", getNamespace("AzureStor"))

is_endpoint_url <- get("is_endpoint_url", getNamespace("AzureStor"))

delete_confirmed <- get("delete_confirmed", getNamespace("AzureStor"))

storage_error_message <- get("storage_error_message", getNamespace("AzureStor"))

process_storage_response <- get("process_storage_response", getNamespace("AzureStor"))
